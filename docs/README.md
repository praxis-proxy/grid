# AI Grid Network (AGN) Documentation

AI Grid Network (AGN), listed as Praxis Grid on the Praxis website, is a
technical preview. The sections below are ordered for a first deployment: get a
grid running, install it on your clusters, operate and route traffic,
understand the design, then look up reference material. Contributor material is
last.

## Understand the Network

A <code>GridNetwork</code> describes participating <code>GridSite</code>
resources and provider state they exchange. Operators publish local routing
configuration; gateways select destinations and carry inference traffic. Grid
does not create site-to-site network connectivity. Grid Gateway's
<code>grid_site_route</code> and Praxis AI's <code>intelligent_route</code>
are separate routing integrations; follow the guide for the path you deploy.

## Getting Started

- [Getting Started](getting-started/README.md): where to begin.
- [Technical Preview Scope and Limitations](technical-preview-scope.md):
  routing scope, deployment-owner responsibilities, and topology boundaries.
- [Quickstart](getting-started/quickstart.md): run a two-site grid on kind,
  send a request through it, and clean up.

## Installation

- [Installation](installation/README.md): installation guides and examples.
- [Existing-Cluster Helm Installation](installation/existing-clusters.md):
  install AGN and Praxis on running Kubernetes clusters with Helm.
- [Site Enrollment](installation/enrollment.md): mint a token, submit a CSR,
  and receive a grid-assigned certificate and gossip material.
- [Installing on AWS](installation/aws.md): load balancers, private
  connectivity, and egress addresses for three single-node clusters.
- [0.3.x CRD Field Migration](installation/migration-0.3.x.md): migrate
  deprecated field names before the 0.3.x upgrade.

## Operations

- [Operations](operations/operations.md): production installation, network
  formation, site lifecycle, routing configuration, security, and
  observability.
- [Adding an Inference Provider](operations/adding-provider.md): step-by-step
  workflow for in-cluster, existing-service, and external HTTPS providers.
- [Provider Draining](operations/provider-draining.md): graceful provider
  maintenance, gateway-wide selection, and reversible drain state.
- [Scaling the Signals Listener](operations/signals-scaling.md): connections
  per peer, listener bounds, polling cost, and the limits to size behind an
  OpenShift router.
- [OpenTelemetry](operations/opentelemetry.md): exporter configuration,
  secret handling, image requirements, and the Praxis 0.7.1 trace-linkage
  limit.
- [FIPS Support](fips.md): FIPS builds, runtime images, and startup
  verification.
- [Fleet Dashboard](https://github.com/praxis-proxy/grid/blob/main/crates/fleet-dashboard/README.md): optional hub web UI
  drawing every site on a map with health from its own Prometheus; deployed
  only when you install its chart.

## Routing

- [Routing Guide](routing.md): choose a routing behavior and configure policy,
  scoring, selection groups, affinity, and selection modes.
- [Tuning cross-site site selection](site-selection.md): how a grid gateway
  chooses a site, what feeds it, and what to change.

## Architecture

- [Overview](architecture/overview.md): operator responsibilities, data-plane
  boundaries, workspace crates, and terminology.
- [Routing Architecture and Overlay Contract](architecture/routing.md):
  overlay format, revision lifecycle, scope, provenance, security, and delivery.
- [Provider Scoring](architecture/scoring.md): metrics input, normalization,
  stale data, and scoring implementation.
- [Prefix Affinity and Stored-State Pinning](architecture/prefix-affinity.md):
  keeping conversations on the site that holds their prompt cache.
- [Signal Propagation](architecture/signals.md): gossip and poll modes for
  cross-site load signals.
- [Polling Cross-Site Load Signals](architecture/polling-metrics.md): the poll
  path, provider readiness, and failure behavior.
- [Signed Gossip Trust](architecture/signed-gossip-trust.md): peer certificate
  policy, certificate bootstrap, signed state verification, and source map.
- [Auth and Policy](architecture/auth.md): provider authentication strategies,
  access policy, and trust model.
- [Enrollment](architecture/enrollment.md): how a site becomes a member: parties,
  the flow, trust boundaries, and the planned object-driven shape.
- [External Client Ingress](architecture/external-ingress.md): GTM/GLB edge
  selection, AGN provider routing, trust boundaries, affinity, snapshot
  delivery, and provider-boundary ownership.

## Reference

- [Custom Resource Definitions](reference/crds.md): complete API fields
  and defaults, including the SiteInvite API and routing configuration.
- [Consumer Config](reference/consumer-config.md): operator-generated
  consumer Praxis `ConfigMap` and the `GatewayRef.praxisConfig` API.
- [Metrics Reference](reference/metrics.md): every metric the operator and the
  gateway export, with types, labels, and what each one means.

## Examples

- [Hub and Site Install](https://github.com/praxis-proxy/grid/blob/main/examples/helm/hub-site/README.md):
  the Helm commands for a hub and one site.
- [Single Gateway Examples](https://github.com/praxis-proxy/grid/blob/main/examples/helm/praxis-gateway/README.md):
  values for one <code>praxis-gateway</code> release, one file per
  <code>praxisConfig.source</code>.
- [Praxis demos](https://github.com/praxis-proxy/demos): self-contained
  examples, including the Grid QuickStarts. Follow each demo's own prerequisites
  and status notes.
- [Experimental demos](https://github.com/praxis-proxy/experimental/tree/main/demos):
  development examples whose status and prerequisites are documented by each
  demo.

## Evaluation and Qualifications

- [Grid evaluation and E2E scenarios](developing/ci-kind-e2e.md): choose a
  runnable scenario by behavior, routing path, and Kind cluster count. The
  guide links to each topology's source-owned prerequisites, command, evidence,
  and cleanup instructions, and summarizes the workflows that currently run.

## Developing

- [Developing AGN](developing/README.md): requirements, workspace map,
  verification matrix, and project management.
- [Development Conventions](developing/conventions.md): coding style, testing
  requirements, documentation rules, and commit attribution.
- [Type Design](developing/type-design.md): shared Praxis guidance for serde,
  enums, newtypes, and representable states.
- [Forge Tooling](developing/forge.md): the pinned Forge that creates test
  clusters.
- [CI Kind E2E](developing/ci-kind-e2e.md): validation tiers, gate sequence,
  sequencing requirements, and environment dependencies.
- [Release Process](developing/release.md): versioning, validation, artifact
  publication, and release workflow.
- [Unicode Audit](developing/unicode-audit.md): tracked text inventory and
  intentional Unicode exceptions.
