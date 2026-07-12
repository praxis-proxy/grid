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
    - grid.cluster-b.example.com:7946
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

**Phase progression note:** `GridNetwork Active` and
`Degraded` phases are not yet implemented.  The
controller emits `Initializing` once TLS secrets are
configured; `Active` progression is deferred until
SWIM mesh integration (OP-06).  The `connected_sites`
status field is hardcoded to `0` until OP-06.  Do not
use `phase: Active` as a readiness signal.

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
(default: `requireAll: true`) leaves room for future
tolerance (partial connectivity, ignoring site types).

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
    strategy: api_key
    secretRef: { name: anthropic-key }
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

**OP-01 (implemented):** The `GridNetwork` controller
renders routing overlay `ConfigMap`s from static CRD
data. For each `gatewayRef` in the `GridNetwork`, it
server-side applies a `ConfigMap` named
`grid-overlay-{network}-{gateway}` containing:

- **`grid-config.json`**: JSON-serialised
  `RoutingOverlay` with one `RoutingCandidate` per
  model per `InferenceProvider` in the network.

The overlay shape is compatible with the Praxis
`grid_route` filter:

```json
{
  "network": "production",
  "local_site": "production",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "granite-3.3-8b",
      "site": "local-vllm",
      "cluster": "local-vllm",
      "fresh": true
    }
  ]
}
```

**Production cluster naming:** `candidate.cluster`
equals the `InferenceProvider` metadata name (the
Kubernetes resource name, not its endpoint).  The
Praxis `load_balancer` cluster serving that provider
must use the same name.

Local development with `xtask env` substitutes
`gateway-{site}` as the cluster name and generates
matching `load_balancer` entries — see
`xtask/src/env/operator_overlay.rs`.  The xtask path
does not validate the production naming contract.

**Future phases:**
- OP-02: `InferenceProvider` status reconciliation
- OP-03: Gateway annotation patching (planned) — not
  yet implemented; see OP-03 in OPERATOR_STATUS.md
- OP-04: Local validation harness reads operator-produced
  `ConfigMap` via `xtask env deploy-consumer-gateway --overlay-config`
- OP-05: Metrics-to-snapshot freshness updates set
  `fresh: false` when metrics are stale
- OP-06: SWIM/CRDT membership and capability
  propagation populates cross-site candidates

## 9. Workloads Consume Providers

Workloads send requests to the Praxis Gateway.
The gateway's grid scoring filter selects the optimal
backend. Praxis handles API translation and credential
injection transparently.

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

`xtask env` is not the production operator:

- It does not reconcile Kubernetes resources
  continuously
- It does not run SWIM membership or CRDT propagation
- It does not manage `GridNetwork`, `GridSite`, or
  `InferenceProvider` CRDs
- It does not perform live config hot-reload against
  a running gateway

In the production architecture, the above are
responsibilities of the Grid Operator and its
controllers. Implementation is staged; `xtask env` is
not a substitute for those controllers.

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
production orchestrator. It should not be treated as
evidence that the corresponding operator reconciliation
loop is complete.

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
