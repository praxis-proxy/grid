# AI Grid Network (AGN)

AI Grid Network (AGN) is a distributed control plane that connects AI
inference backends across Kubernetes clusters, cloud
providers, and third-party APIs into a single routable
mesh. AGN figures out where models are, which backends
are healthy, and which one should handle the next
request - then tells the [Praxis] gateway how to route.

[Praxis]:https://github.com/praxis-proxy/praxis

## How It Works

The AGN Operator is an orchestrator, not a proxy. It watches
Kubernetes resources, discovers peer sites over a
gossip protocol (SWIM), propagates provider state
with CRDTs, scores candidates, and writes a routing
overlay that Praxis consumes at request time.

```text
+---------------------------+     +---------------------------+
|  Site A (Kubernetes)      |     |  Site B (Kubernetes)      |
|                           |     |                           |
|  +---------------------+  |     |  +---------------------+  |
|  | AGN Operator        |  |     |  | AGN Operator        |  |
|  | - SWIM membership   |  |     |  | - SWIM membership   |  |
|  | - CRDT state sync   |  |     |  | - CRDT state sync   |  |
|  | - scoring engine    |  |     |  | - scoring engine    |  |
|  | - overlay renderer  |  |     |  | - overlay renderer  |  |
|  +--------+------------+  |     |  +--------+------------+  |
|           |               |     |           |               |
|           | ConfigMap     |     |           | ConfigMap     |
|           v               |     |           v               |
|  +---------------------+  |     |  +---------------------+  |
|  | Praxis AI Gateway   |  |     |  | Praxis AI Gateway   |  |
|  | - request routing   |  |     |  | - request routing   |  |
|  | - API translation   |  |     |  | - API translation   |  |
|  | - credential inject |  |     |  | - credential inject |  |
|  +--------+------------+  |     |  +--------+------------+  |
|           |               |     |           |               |
|           v               |     |           v               |
|  +---------------------+  |     |  +---------------------+  |
|  | Inference Backends  |  |     |  | Inference Backends  |  |
|  | (llm-d, vLLM, etc.) |  |     |  | (Bedrock, Vertex,   |  |
|                           |     |  | OpenAI, Anthropic)  |  |
|  +---------------------+  |     |  +---------------------+  |
+---------------------------+     +---------------------------+
```

The gateways communicate over mTLS.

AGN Operators exchange membership and provider state over SWIM and CRDT
replication.

AGN handles the **control plane** (what should be
routable). Praxis handles the **data plane** (routing
and proxying actual requests).

## Key Concepts

**GridNetwork** - defines a logical mesh of sites.
Holds SWIM seeds, TLS settings, and gateway
references.

**GridSite** - represents one participating cluster
or location. Created automatically from SWIM
discovery or manually for seed peers.

**InferenceProvider** - declares model capacity at a
site: model name, backend kind (self-hosted,
cloud-managed, or API provider), health config, and
auth strategy.

**Routing overlay** - a versioned ConfigMap that AGN
writes for each gateway. Contains scored candidates,
cluster definitions with mTLS config, and credential
references. Praxis hot-reloads this without restarts.

**Scoring** - AGN applies one provider-level strategy before
writing the overlay. `noMetrics` is the generic default for
external APIs and providers without comparable telemetry.
llm-d pools can opt into `queueDepth` or `kvCachePressure`.
Request-specific prefix affinity remains inside llm-d EPP,
which selects a pod after AGN selects a provider pool.

See the [AGN Routing Guide](docs/routing.md) for configuration by routing
need, selection groups, request-time selection modes, affinity, and runnable
examples.

## Request Flow

Once the overlay is loaded, a request flows through
two gateway pipelines:

```text
client request
  -> Praxis consumer/edge gateway
  -> intelligent_route selects a provider from overlay
  -> gateway-to-gateway mTLS
  -> Praxis provider gateway authenticates the peer
  -> provider_route validates the selected candidate
  -> credential_inject adds backend auth
  -> load_balancer picks a backend instance
  -> response returns to the client
```

AGN is never in the request path. All routing
decisions use a pre-computed local overlay file.

## Install

```console
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <version> \
  --namespace grid-system \
  --create-namespace
```

See the
[chart documentation](charts/grid-operator/README.md)
for values, RBAC, CRD upgrades, and SWIM service
exposure. Install a compatible
[Praxis](https://github.com/praxis-proxy/praxis)
gateway separately.

For Kustomize or raw manifests, see
[deploy/](deploy/README.md).

## Getting Started

[Praxis demos](https://github.com/praxis-proxy/demos): deployable demonstrations
with automated runtime
proofs of routing, failover, security boundaries,
and provider lifecycle.

[Existing-cluster installation](docs/installation/existing-clusters.md): install
AGN and Praxis on running Kubernetes clusters with Helm.

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `operator` | K8s controllers, CRDs, operator binary |
| `scoring` | Strategy-selected scoring engine and grid state |
| `certs` | Certificate generation and mTLS provider trait |
| `swim` | foca SWIM wrapper and encryption |
| `crdt` | Delta CRDT types (LWW, OR-Set, G-Counter) |
| `overlay-sync` | Sidecar for fast ConfigMap-to-file delivery |
| `mock-providers` | Mock OpenAI, Anthropic, Bedrock, Vertex APIs |
| `xtask` | Dev task runner for multi-cluster test environments |
| `fleet-dashboard` | Opt-in hub web UI: fleet map and per-site health from each site's Prometheus (Axum + React) |

## Project name and compatibility

AI Grid Network (AGN) is the human-facing project name used in documentation.
Established software identifiers remain unchanged:
the Rust package and binary are `operator`, the deployed operator is
`grid-operator`, the operator chart is `grid-operator`, and the gateway chart is
`praxis-gateway`. The API group is `grid.praxis.fast`, with kinds such as
`GridNetwork` and `GridSite`. It replaced `grid.praxis-proxy.io`, along with the
prefix of every Grid label and annotation key, so custom resources created under
the old group must be recreated. The project offers no migration guarantees at
this stage, so across the rename the supported path is a fresh install: uninstall,
delete the `grid.praxis-proxy.io` CRDs, and install again. An upgrade
deletes the old CRDs, and every resource under them, unless the previous release
installed them with the `helm.sh/resource-policy: keep` annotation, which `crds.keep`
set at that time. Existing
`grid-*` resource names, `GRID_*` environment variables, metrics, and
configuration fields are unchanged.
Existing downstream names, including `praxis-ai-grid-operator` where used, are
also unchanged. Downstream naming alignment is deferred to a separate effort.

## Development

Requires Rust stable 1.96+, Rust nightly (for
rustfmt), and Docker/Podman + kind for integration
tests.

```console
make build          # workspace build
make test           # all tests
make lint           # clippy + fmt check + machete
make audit          # cargo audit + cargo deny check
make all            # build + fmt + lint + test + audit
```

Install the pinned upstream Forge with `./scripts/forge.sh install` before
running environment qualifications. See [Forge tooling](docs/developing/forge.md)
for provenance, overrides, and upgrade checks.

See the [development guide](docs/development.md) and
[conventions](docs/conventions.md) for full details.

## Documentation

- [Technical preview scope and limitations](docs/technical-preview-scope.md)
- [Architecture overview](docs/architecture/overview.md)
- [Custom resources](docs/architecture/crds.md)
- [Routing Guide](docs/routing.md)
- [Routing Architecture and Overlay Contract](docs/architecture/routing.md)
- [Scoring](docs/architecture/scoring.md)
- [Auth and policy](docs/architecture/auth.md)
- [Operations](docs/architecture/operations.md)
- [Consumer config](docs/architecture/consumer-config.md)
- [Documentation index](docs/README.md)

## License

Apache-2.0
