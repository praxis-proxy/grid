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
- [Static Provider Weighting](architecture/static-weighting.md) — configured
  relative capacity, policy precedence, cross-site propagation, and weighted
  request selection.
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

## Examples

- [Example Scenarios 2026](examples-2026.md) - progressive scenarios covering
  multi-cluster routing, provider fallback, load balancing, affinity, policy,
  discovery, federation, and partition handling.
- [Provider Traffic Selection](../tests/e2e/topologies/grid-provider-traffic/README.md) —
  runnable three-cluster topology for Grid selection groups and request-time
  round-robin provider choice.
- [Grid QuickStarts](https://github.com/praxis-proxy/demos) — deployable
  examples with automated runtime proofs of routing, failover, security
  boundaries, and provider lifecycle.

## Integration Qualifications

- [Provider Traffic Qualification](../tests/e2e/topologies/grid-provider-traffic/README.md) -
  proves multi-cluster discovery, accepted-overlay delivery, request-time
  round-robin selection, provider attribution, and stable routing.
- [Static Weighted Provider Qualification](../tests/e2e/topologies/grid-static-weighted/README.md) -
  proves configured capacity propagation, weighted overlay convergence,
  proportional request selection, hot reload, and equal-weight recovery.
- [Distributed Token Quota Qualification](../tests/e2e/topologies/grid-token-rate-limit/README.md) -
  proves shared identity-scoped quota enforcement across gateway replicas,
  regional provider selection, expiry, restart persistence, fail-closed state
  storage, and storage-network isolation.
- [Single-cluster Multi-gateway Qualification](../tests/e2e/topologies/grid-single-cluster-multi-gateway/README.md) -
  proves shared overlay delivery and independent consumer/provider gateway
  behavior within one Kind cluster and one GridSite.
- [Provider draining](architecture/provider-draining.md) - documents graceful
  provider maintenance, gateway-wide selection, and reversible drain state.

These integration tests create their environments through Forge and execute
through first-class Rust `xtask` commands. Their topology READMEs document
image preparation, execution, evidence, and cleanup.

## Installation

- [Existing-Cluster Helm Installation](installation/existing-clusters.md) —
  install Grid and Praxis on running Kubernetes clusters with Helm.

## Development

- [Release Process](release.md) - versioning, validation, artifact publication,
  and release workflow.
- [Development](development.md) — build, test, format, lint, and coverage.
- [Conventions](conventions.md) — coding style, testing requirements,
  documentation rules, and commit attribution.
- [Developing: Conventions](developing/conventions.md) — shared Praxis coding,
  tracing, testing, and review conventions.
- [Developing: Type Design](developing/type-design.md) — shared Praxis guidance
  for serde, enums, newtypes, and representable states.
