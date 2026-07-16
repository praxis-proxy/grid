# Grid Operations Walkthrough

Step-by-step guide to grid formation, site lifecycle,
and routing configuration.

## 1. Deploy the Grid Operator

Install CRDs and the operator Deployment. The operator
starts its controller manager. No SWIM runtime starts
until a `GridNetwork` resource exists.

```console
kubectl apply -f grid-crds.yaml
kubectl apply -f operator.yaml
```

The operator runs as a single binary with multiple
controllers (one per CRD type) in the same process.

## 2. Create a GridNetwork

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridNetwork
metadata:
  name: production
spec:
  seeds:
    - "10.0.0.5:7946"
  gatewayRefs:
    - name: inference-gw
      namespace: praxis-system
  tls:
    caSecretRef:
      name: grid-ca
      namespace: praxis-system
    siteSecretRef:
      name: grid-site-cert
      namespace: praxis-system
```

The GridNetwork controller:
1. Generates a grid CA via `certs`
2. Generates this site's certificate (DNS SAN:
   `{site-name}.grid.internal`, dual EKU for mTLS)
3. Stores both in Kubernetes Secrets
4. Generates a SWIM encryption key
5. Starts the SWIM runtime with seed peers
6. Sets `status.phase: Initializing`

### CRD-driven seeds

`spec.seeds` is now **operator-consumed**: on every `GridNetwork` reconcile the
controller parses the seed list, filters invalid addresses (logged at `warn`,
no reconcile failure), removes the local advertise address to prevent self-
announce noise, deduplicates, and calls `SwimHandle::announce_seeds` to deliver
the batch to the running SWIM event loop.  Re-announcing to already-connected
peers is idempotent — foca ignores redundant joins.

Startup seeds from `GRID_SWIM_SEEDS` (env var, set by `xtask`) and CRD seeds
are additive.  The env var seeds run once at startup; CRD seeds run on every
reconcile so dynamically added addresses take effect without an operator restart.

**Global-runtime semantics**

The SWIM runtime is process-global — one UDP listener per operator process,
shared across all `GridNetwork` reconciles.  Seeds from any
`GridNetwork.spec.seeds` are announced to the same SWIM membership node.
This is site-membership bootstrap, not per-network membership isolation.
CRDT provider records remain network-scoped separately.

**Channel-full retry**

If the seed announce channel is full (capacity 16 batches), the announce is
skipped for the current reconcile and retried on the next
(`REQUEUE_INTERVAL = 300 s`).  Seeds are not guaranteed to be applied
immediately under heavy broadcast load.

**Limitations**
- Seed removal is not tracked: removing an address from `spec.seeds` stops
  re-announcing to it but does not evict an already-connected peer.
- Seeds must be `IP:port` socket addresses; DNS names are not resolved.
  Example valid value: `10.0.0.2:7946`.

**Phase progression:** `GridNetwork Active` is set when
the SWIM runtime reports at least one `Alive` peer in
its `MembershipSnapshot`.  `Degraded` is set when peers
are known but all are `Suspect` or `Dead`.
`connectedSites` reflects the live SWIM `Alive` peer
count; `distributedProviderCount` reflects remote
`InferenceProvider` records received via SWIM CRDT
broadcast.

Both fields are `0` and the phase remains `Pending` or
`Initializing` when SWIM is disabled (i.e. the operator
is started without `GRID_SWIM_BIND_ADDR`).

## 3. Sites Discover Each Other

When the SWIM runtime contacts a seed peer:

**Grid ID negotiation**:
- Neither site has a `gridId`: deterministic tie-break
  (lexicographic site name), winner generates UUID,
  other adopts it
- Remote has a `gridId`, local doesn't: local adopts it
- Both have the same `gridId`: normal join
- Both have different `gridIds`: connection rejected
  (separate grids)

The operator creates a `GridSite` resource for the
discovered peer.

`GridSite` status: `phase: Discovered`

## 4. mTLS Certificate Exchange

For each Discovered `GridSite`:
1. Local operator sends its public cert PEM via SWIM
   piggyback broadcast (foca `BroadcastHandler`)
2. Remote operator receives and stores the cert
3. Each side appends the other's cert to their trust
   bundle Secret

`GridSite` status: `phase: Connecting`

## 5. Connectivity Verification

The operator verifies three conditions:

| Condition | Check |
|-----------|-------|
| `SWIMReachable` | SWIM probes succeeding |
| `MTLSEstablished` | mTLS handshake passes |
| `DataPlaneHealthy` | HTTP ping through Praxis |

A `connectivityPolicy` field on `GridNetwork`
(default: `requireAll: true`) controls how strictly the
operator evaluates partial connectivity and site-type
specific checks.

## 6. Capability Negotiation

Before reaching Active, sites exchange capability
summaries via SWIM piggyback:
- Which provider types they offer (inference, tools,
  agents)
- Provider counts per type

The `GridSite` status `capabilities` field is updated.
At least one provider type must be available.

`GridSite` status: `phase: Active`

## 7. Register Providers

Users or auto-discovery create provider resources.
See the [CRDs doc](crds.md) for full specs.

Example — an API provider:
```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: InferenceProvider
metadata:
  name: anthropic-api
spec:
  gridNetworkRef: production
  providerKind: anthropic
  backendKind: api_provider
  endpoint: https://api.anthropic.com
  models:
    - name: claude-sonnet-4
  auth:
    strategy: bearer_token
    secretRef:
      name: anthropic-token
      namespace: praxis-system
      key: token
  accessPolicy:
    siteSelector: {}
```

Example — a local llm-d cluster:
```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: InferenceProvider
metadata:
  name: local-vllm
spec:
  gridNetworkRef: production
  providerKind: self_hosted
  backendKind: local
  endpoint: http://vllm-service.inference:8000
  models:
    - name: llama-3.2-8b
```

## 8. Routing Configuration

The `GridNetwork` controller renders routing overlay
`ConfigMap`s from CRD data. For each `gatewayRef` in the
`GridNetwork`, it server-side applies a `ConfigMap`
named `grid-overlay-{network}-{gateway}` containing:

- **`grid-config.json`**: JSON-serialised
  `RoutingOverlay` with one `RoutingCandidate` per
  model per `InferenceProvider` in the network.  When
  `spec.auth.secretRef` is set, candidates carry only
  the credential reference, never token bytes.

The overlay shape is compatible with the Praxis
`grid_route` filter:

```json
{
  "network": "production",
  "local_site": "production",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "claude-sonnet-4",
      "site": "anthropic-api",
      "cluster": "anthropic-api",
      "fresh": true,
      "credential": {
        "strategy": "bearer_token",
        "secretRef": {
          "name": "anthropic-token",
          "namespace": "praxis-system",
          "key": "token"
        }
      }
    }
  ]
}
```

**Cluster naming:** `candidate.cluster` uses
`spec.routingClusterRef` when set, otherwise the
`InferenceProvider` metadata name.  The Praxis
`load_balancer` cluster serving that provider must use
the same identity.

Local development with `xtask env` maps overlay site
identities to generated `gateway-{site}` load-balancer
entries; see `xtask/src/env/operator_overlay.rs`.

## 9. Workloads Consume Providers

Workloads send requests to the Praxis Gateway.
The gateway's grid scoring filter selects the optimal
backend. Praxis handles API translation and credential
injection transparently.

For API-provider routes, the request-time path is:

```text
grid_route
  -> writes grid.route.credential.* metadata from the selected candidate
grid_credential_inject
  -> reads the matching token from a mounted Secret file
  -> injects Authorization: Bearer <token>
load_balancer
  -> forwards to the selected provider cluster
```

The token is not stored in the Grid overlay or consumer
Praxis `ConfigMap`.

See [Auth & Policy](auth.md) for workload access
patterns and authentication strategies.

## Site Departure

**Graceful leave**: Operator sends SWIM leave message.
Peers remove the site from membership immediately.
`GridSite` deleted.

**Crash**: SWIM probe fails (direct + indirect).
Site enters suspect state (default 10s timeout).
If no refutation, declared dead. `GridSite` status:
`Active → Unreachable → Left`.

## Adding a New Site to an Existing Grid

1. Deploy the Grid Operator on the new cluster
2. Create a `GridNetwork` with any existing cluster
   as a seed
3. SWIM discovers the existing cluster, which shares
   the membership list of all other sites
4. The new site automatically discovers all grid
   members within seconds
5. mTLS exchange and capability negotiation proceed
   with each peer
6. Once Active, the new site's providers are visible
   to all other sites

## Local kind environment orchestration

The `xtask env` commands provide a local development
and integration-validation path using `kind` clusters.
They are **not** the production reconciliation model.

This path is intended for:

- Local development iteration against a multi-cluster
  topology
- Integration validation before pushing to a real cluster
- CI pipelines that require a running kind environment

### What `xtask env` does

`xtask env` commands are imperative and config-driven.
They operate from `tests/env/config.toml` (or a supplied
`--config` path), which declares clusters, their roles,
and the models each provider cluster exposes.

Available commands:

| Command | What it does |
|---|---|
| `cargo xtask env up` | Creates kind clusters, deploys the configured provider backend, generates local test certificates |
| `cargo xtask env down` | Tears down kind clusters and removes generated certs |
| `cargo xtask env status` | Reports cluster, provider, and cert readiness |
| `cargo xtask env verify-providers` | Probes Chat Completions endpoints against the configured provider backend in all provider clusters |
| `cargo xtask env build-gateway-images` | Builds the Praxis AI gateway and mock EPP container images |
| `cargo xtask env load-gateway-images` | Loads locally-built images into kind cluster nodes |
| `cargo xtask env deploy-provider-gateways` | Applies generated Praxis AI gateway resources to provider clusters |
| `cargo xtask env verify-provider-gateways` | Runs end-to-end probes through the provider gateway request path |
| `cargo xtask env deploy-consumer-gateway` | Deploys a consumer Praxis AI gateway with a generated static `grid_route` config |
| `cargo xtask env deploy-consumer-gateway --overlay-config <path>` | Deploys the consumer gateway using a `grid-config.json` routing overlay file |
| `cargo xtask env verify-gateway-e2e` | Verifies consumer-to-provider routing end-to-end |
| `cargo xtask env verify-mtls-trust` | Verifies provider gateway mTLS enforcement (positive + negative cases) |
| `cargo xtask env verify-api-fallback-native` | Verifies native `grid_route` → `grid_credential_inject` credential injection with token bytes absent from overlay and consumer ConfigMap |
| `cargo xtask env verify-stale-gc-ttl` | Verifies `GridNetwork.spec.staleCandidateTtlSeconds` evicts stale remote candidates from the rendered overlay |
| `cargo xtask env verify-crd-schema` | Verifies required generated CRD schema fields without requiring kind clusters |
| `cargo xtask env validate-all` | Runs the local validation suite and prints a Markdown result table |

### Operator and SWIM local validation

The operator is **not** running inside kind; it connects
to the kind cluster via the local kubeconfig.  SWIM
runtimes use localhost UDP sockets between local operator
processes.  This avoids requiring an operator container
image or in-cluster RBAC for local validation.

#### Setup (one-time per machine)

```console
cargo xtask env up -c tests/env/operator-routing.toml
cargo xtask env load-gateway-images -c tests/env/operator-routing.toml
```

Creates `grid-site-a` (provider, mock-openai backend)
and `grid-consumer` kind clusters, generates local mTLS
certificates, and loads Praxis AI gateway images.

#### CRD schema validation

```console
cargo xtask env verify-crd-schema
```

This command runs the CRD generator and verifies the
generated schema contains required Grid status and
InferenceProvider routing and metrics fields. It does
not require kind clusters.

#### Routing validation

```console
cargo xtask env validate-operator-routing -c tests/env/operator-routing.toml
```

This command deploys the Praxis provider gateway, spawns
the operator out of cluster, applies `GridNetwork` and
`InferenceProvider` fixtures, waits for reconciliation,
exports the operator overlay, deploys the consumer
gateway from that overlay, and sends live HTTP requests
through the consumer gateway.

The validation covers provider health classification,
candidate ordering, metrics-aware ordering,
`routingClusterRef` identity mapping, overlay export,
consumer gateway deployment, successful routing for a
known model, and clean failure for an unknown model.

#### SWIM membership

```console
cargo xtask env verify-swim-membership -c tests/env/operator-routing.toml
```

This command starts two out-of-cluster operator
processes with distinct localhost UDP ports. The
secondary seeds on the primary. After a convergence
window, the command applies a `GridNetwork` fixture and
polls `GridNetwork.status` for SWIM-derived membership
state.

#### CRDT-over-SWIM state

```console
cargo xtask env verify-swim-state -c tests/env/operator-routing.toml
```

This command starts two SWIM-enabled operator processes,
waits for gossip convergence, then applies a
`GridNetwork` and an `InferenceProvider`. Each operator
maps the `InferenceProvider` CRD to a
`crdt::ProviderState` and publishes it as a
`StateBroadcast` over foca's custom-broadcast path. The
receiver merges the `GridStateSnapshot`, and subsequent
status reconciliation reflects remote provider state in
`GridNetwork.status.distributedProviderCount`.

**Provider fields propagated over SWIM:**

| CRDT field | Source |
|---|---|
| `network_id` | owning `GridNetwork.metadata.name` |
| `site_id` | local SWIM site identity |
| `provider_id` | `metadata.name` |
| `routing_cluster` | `spec.routingClusterRef` or `metadata.name` |
| `models` | `spec.models[*].name` |
| `backend_kind` | `spec.backendKind` |
| `phase` | `status.phase` (including `Unavailable`) |
| `metrics` | `metricsConfig` scrape results, or defaults |
| `revision` | `metadata.resourceVersion`, falling back to `metadata.generation` |
| `writer_id` | local SWIM site identity |

`distributedProviderCount` in `GridNetworkStatus`
reflects received remote provider records for the
current `GridNetwork`; local records and records from
other `GridNetwork`s are excluded. The local validation
fixture expects exactly one remote provider record; zero
means state did not arrive, and more than one indicates
cross-network leakage or stale test state.

#### Full local validation suite

```console
cargo xtask env validate-all -c tests/env/operator-routing.toml
```

This command runs the local status check, operator
routing validation, SWIM membership validation,
CRDT-over-SWIM state validation, and mTLS trust
validation in sequence. It continues after individual
step failures and prints a Markdown summary table at the
end so CI logs and manual runs show the complete state
of the environment.

### Required local images

Before running `load-gateway-images`, the following
images must exist in the local container daemon:

| Image | Built from | Required for |
|---|---|---|
| `localhost/praxis-ai:llmd-ext-proc` | AI repository external checkout | All provider and consumer gateways |
| `localhost/praxis-ai-mock-epp:latest` | AI repository external checkout | All provider gateways |
| `grid-mock-providers:latest` | This repository, `mock-providers/Containerfile` | Provider clusters with `backend = "mock-openai"` only |

Use `build-gateway-images --ai-repo <path>` to build the first two images from
the AI repository source tree. Build `grid-mock-providers:latest` separately
from this repository:

```bash
docker build -t grid-mock-providers:latest -f mock-providers/Containerfile .
```

### What `xtask env` does NOT do

The `xtask env up/down/status/deploy-*` commands are
not the production operator:

- They do not reconcile Kubernetes resources
  continuously
- They do not manage `GridNetwork`, `GridSite`, or
  `InferenceProvider` CRDs in a watch loop
- They do not perform live config hot-reload against
  a running gateway

The `verify-swim-membership` and `verify-swim-state`
commands do spawn out-of-cluster operator processes that
run real SWIM and CRDT reconciliation, but they use
localhost UDP sockets and ephemeral fixtures — they are
not a substitute for in-cluster production deployment.

In the production architecture, continuous reconciliation
is the responsibility of the Grid Operator and its
controllers. `xtask env` commands are a validation
convenience layer, not a production orchestrator.

### Routing overlay file input

`deploy-consumer-gateway --overlay-config <path>`
accepts a `grid-config.json` routing overlay file. This
allows local validation of the overlay wire format and
consumer gateway config generation without running a
full production operator reconcile loop. The overlay
file format is:

```json
{
  "network": "<grid-network-name>",
  "local_site": "<consumer-site-name>",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "<model-name>",
      "site": "<provider-site-name>",
      "cluster": "<overlay-cluster-name>",
      "fresh": true
    }
  ]
}
```

When an overlay is supplied, `grid_route.local_site`
and candidates come from the overlay.  The
`load_balancer` section is still generated from the
provider endpoints in the environment config.

### Separation from production reconciliation

The production architecture is operator-driven. The
Grid Operator reconciliation path owns long-lived
management of:

- `GridNetwork`, `GridSite`, and `InferenceProvider`
  CRD reconciliation
- SWIM mesh formation and certificate lifecycle
- Routing overlay ConfigMap generation and application

`xtask env` is a development convenience layer that
uses the same config and cert infrastructure, not a
production orchestrator. Production reconciliation
semantics are defined by the Grid Operator controllers,
not by the imperative `xtask env` command flow.

### Opinionated walkthroughs and topology fixtures

Scripts, static manifests, and walkthrough
documentation for specific gateway-to-gateway
topologies are maintained outside this repository
at:

```
nerdalert/praxis-research-spikes/demo/ai-grid-gateway-to-gateway/
```

Grid keeps generic, config-driven, reusable commands.
Topology-specific fixtures, static manifests, and
presentation walkthroughs belong in the
research-spikes repository.
