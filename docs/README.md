# AI Grid Network (AGN) Documentation

- [Technical Preview Scope and Limitations](technical-preview-scope.md):
  routing scope, deployment-owner responsibilities, and topology boundaries.

## Routing

- [Routing Guide](routing.md): choose a routing behavior and configure policy,
  scoring, selection groups, affinity, and selection modes.
- [Tuning cross-site site selection](site-selection.md): how a grid gateway
  chooses a site, what feeds it, and what to change.
- [Routing Architecture and Overlay Contract](architecture/routing.md):
  overlay format, revision lifecycle, scope, provenance, security, and delivery.
- [Provider Scoring](architecture/scoring.md): metrics input, normalization,
  stale data, and scoring implementation.
- [Custom Resource Definitions](architecture/crds.md): complete API fields
  and defaults, including routing configuration.

## Architecture

- [Overview](architecture/overview.md) — operator responsibilities, data-plane
  boundaries, workspace crates, and terminology.
- [Auth and Policy](architecture/auth.md) — provider authentication strategies,
  access policy, and trust model.
- [Consumer Config](architecture/consumer-config.md) — operator-generated
  consumer Praxis `ConfigMap` and the `GatewayRef.consumerConfig` API.
- [OpenTelemetry](architecture/opentelemetry.md) - exporter configuration,
  secret handling, image requirements, and the Praxis 0.7.1 trace-linkage limit.
- [External Client Ingress](architecture/external-ingress.md) — GTM/GLB edge
  selection, AGN provider routing, trust boundaries, affinity, snapshot
  delivery, and provider-boundary ownership.

## Operations

- [Fleet Dashboard](../crates/fleet-dashboard/README.md) — optional hub web UI drawing
  every site on a map with health from its own Prometheus; deployed only when
  you install its chart.
- [Adding an Inference Provider](adding-provider.md) — step-by-step
  workflow for in-cluster, existing-service, and external HTTPS providers.
- [Metrics Reference](metrics.md) — every metric the operator and the gateway
  export, with types, labels, and what each one means.
- [Operations](architecture/operations.md) — local environment setup,
  validation commands, and operator workflows.
- [CI Kind E2E](architecture/ci-kind-e2e.md) — validation tiers, gate sequence,
  sequencing requirements, and environment dependencies.

## Examples

- [Provider Traffic Selection](../tests/e2e/topologies/grid-provider-traffic/README.md) —
  runnable three-cluster topology for AGN selection groups and request-time
  round-robin provider choice.
- [Praxis demos](https://github.com/praxis-proxy/demos): deployable
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
  install AGN and Praxis on running Kubernetes clusters with Helm.

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
