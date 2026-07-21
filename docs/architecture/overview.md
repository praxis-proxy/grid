# Architecture Overview

The AI Grid is a Kubernetes control plane for AI inference routing across
clusters, cloud providers, and third-party APIs.

Grid owns CRDs, reconciliation, SWIM/CRDT state distribution, scoring, routing
overlay rendering, and certificate/Secret reference orchestration.  It does not
proxy request traffic.

## Component Stack

Grid sits above the Praxis data plane:

| Layer | Role |
|-------|------|
| **Grid Operator** | Kubernetes control plane. Watches Grid CRDs, exchanges provider state with peers, scores candidates, and renders routing overlays. |
| **Praxis AI** | AI-aware gateway/data plane. Runs request parsing, `grid_route`, optional `grid_credential_inject`, load balancing, TLS, and upstream proxying. |
| **Praxis Core** | Generic proxy/filter runtime. Provides reusable filters such as load balancer, `endpoint_selector`, `ext_proc`, and `peer_identity_trust`. |
| **Pingora** | Low-level async proxy engine. Handles TCP/TLS, connection pooling, HTTP codecs, and upstream I/O underneath Praxis. |

The separation is intentional: Grid prepares routing state; Praxis AI executes
the request path.

## What Grid Does and Does Not Do

Grid does:

- watch `GridNetwork`, `GridSite`, and `InferenceProvider` resources
- form a SWIM mesh with peer Grid operators
- publish and merge CRDT provider/site snapshots
- score provider candidates
- render `grid-config.json` overlay `ConfigMap`s
- project credential references, never credential values
- generate and manage Grid TLS material

Grid does not:

- proxy HTTP requests
- translate API formats
- run Praxis filters
- inject provider credentials at request time
- write provider token values into overlays, generated `ConfigMap`s, status, or
  logs

## CRDs

API group: `grid.praxis-proxy.io/v1alpha1`.  The current CRDs are
cluster-scoped.

| CRD | Current role |
|-----|--------------|
| `GridNetwork` | Top-level mesh configuration: SWIM seeds, TLS settings, gateway references, and optional consumer config generation. |
| `GridSite` | One cluster/site in the mesh. Tracks discovery, gateway address, public cert material, trust fingerprint, and phase. |
| `InferenceProvider` | One inference backend declaration: model name, backend kind, endpoint, health config, auth strategy, and provider status. |

The inference path is the implemented and validated reconciled path today.

`AgentToolProvider` and `AgentToAgentProvider` are schema direction for MCP and
A2A.  Their CRD types exist, but the operator does not currently run controllers
or render complete routed paths for them.

See [CRDs](crds.md) for field and status details.

## SWIM and CRDT

SWIM and CRDT are the control-plane state distribution layer.

SWIM, via `foca`, answers:

```text
Which peer Grid operators are alive?
```

CRDT state answers:

```text
What provider/site state has each peer advertised?
```

Neither SWIM nor CRDT makes authorization decisions.  SWIM discovery alone does
not make a site routable.

Provider snapshots include the network, advertising site, provider identity,
routing cluster, model list, backend kind, lifecycle phase, optional metrics,
access policy, and revision metadata.

## Local Operator View

Each operator renders overlays for its own local gateways from that operator's
current merged view:

```text
local Kubernetes CRDs
+ local observed provider/site status
+ remote provider/site state received over SWIM/CRDT
= this operator's local routing view
```

Different sites should converge, but they are not guaranteed to have identical
views at every instant.  Each operator renders from the state it has received
and merged so far.

## Routing Overlay

For each gateway reference on a `GridNetwork`, the operator writes a routing
overlay `ConfigMap` containing `grid-config.json`.

That overlay contains:

- the local site name for that gateway
- routable model/provider candidates
- candidate site and cluster identities
- freshness and ordering information
- optional credential references

Credential references are only references:

```text
strategy + Secret name + namespace + key
```

Token bytes are not written into the overlay.

Overlay regeneration is reconcile-driven, not request-driven.  See
[When grid-config.json regenerates](routing.md#when-grid-configjson-regenerates)
for the trigger list.

## Scoring

Grid scores and orders candidates before writing the overlay.

The current weighted signals are:

| Signal | Weight | Source |
|--------|--------|--------|
| Locality | 3.0 | Configured backend category and region context |
| Queue depth | 3.0 | Prometheus scrape or CRDT metrics |
| KV-cache utilization | 2.0 | Prometheus scrape or CRDT metrics |
| Prefix-cache hit ratio | 2.0 | Prometheus scrape or CRDT metrics |
| Latency | 2.0 | Metrics/local observation |
| Cost | 1.0 | Provider config |

Higher score sorts earlier.  `Unavailable` providers are excluded.  `Degraded`
or stale providers may remain in the overlay with `fresh: false` and are sorted
behind equal-scored fresh candidates.  Remaining ties are deterministic by
candidate identity.

At request time, `grid_route` consumes the pre-rendered order.  It does not
recompute the full scoring formula.

See [Scoring](scoring.md) for the full scoring model and metrics normalization
contract.

## Credentials

Credential handling follows the final-hop rule:

```text
The gateway or provider-side component that makes the final backend call owns
and injects the backend credential.
```

Grid validates `InferenceProvider.spec.auth.secretRef` and projects only a
credential reference into the overlay.  Praxis AI performs request-time
injection with `grid_credential_inject` on the gateway that has the relevant
Secret mounted.

For direct API-provider or cloud-provider fallback, the consumer gateway is
often also the final-hop gateway.  For remote Grid sites, provider credentials
stay in the remote provider site or provider-side component.  Grid does not copy
Secret values across clusters.

See [Authentication and Access Policy](auth.md) for the credential lifecycle.

## End-to-end Request Flow

```text
Kubernetes CRDs + SWIM/CRDT state
  → Grid operator reconciles
  → Grid renders the local gateway's grid-config.json ConfigMap
  → Praxis AI gateway loads the overlay
  → request arrives at the consumer gateway
  → request-format filter extracts the model
  → grid_route selects a provider cluster from the loaded overlay
  → optional grid_credential_inject adds provider auth at the final hop
  → load_balancer selects the upstream endpoint
  → Pingora sends the request upstream
  → response returns to the client
```

The request hot path does not call Kubernetes, SWIM, or the Grid operator.
Praxis AI routes from local, already-loaded config.

## Readiness Caveats

`GridSite.status.phase == Active` is a control-plane eligibility signal.  It
means Grid has enough site/trust information to consider the site for overlay
generation: the configured fingerprint matched and the TCP probe passed.

It does not prove that a Praxis gateway has completed an mTLS handshake,
accepted client identity, loaded the latest routing config, or authorized
provider-side traffic.  Data-plane readiness is enforced separately by gateway
filters and deployment health.

Rendering a new `ConfigMap` also does not prove the gateway has loaded it.
Praxis gateways do not automatically hot-reload from a changed `ConfigMap`
volume mount; a pod restart, rollout, or explicit reload path is required.  See
[Consumer Config](consumer-config.md#reload-and-rollout).

Praxis AI overlay hot reload is work in progress.  Dynamic scoring is only
useful for live traffic once Praxis AI can reload the overlay cheaply without
restarting the pod.  Restarting gateways on every scoring or membership change
would turn routing churn into deployment churn.

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `operator` | Kubernetes controllers, CRDs, overlay rendering, operator binary |
| `scoring` | Candidate scoring engine and backend metric types |
| `crdt` | Delta CRDTs: LWW Register, OR-Set, G-Counter |
| `swim` | SWIM membership wrapper around `foca` |
| `certs` | CA and site certificate generation |
| `mock-providers` | Mock OpenAI/Anthropic/Bedrock/Vertex-style providers for tests |
| `xtask` | Local multi-cluster validation harness |

Dependency shape:

```text
operator ──→ scoring
          ├─→ certs
          ├─→ swim ──→ foca
          └─→ crdt

crdt, scoring, certs: standalone crates
```

## Backend Categories

Grid currently models these inference backend categories:

| Backend kind | Meaning |
|--------------|---------|
| `local` | Self-hosted capacity in the local Grid site. |
| `remote` | Self-hosted capacity reached through another Grid site. |
| `cloud_managed` | Cloud inference controlled by the platform, such as Bedrock or Vertex. |
| `api_provider` | Third-party hosted API, such as OpenAI or Anthropic. |

These categories affect scoring, credential placement, and transport
expectations.

## Integration with Praxis

Grid and the Praxis deployment owner configure different parts of the gateway:

| Concern | Owner |
|---------|-------|
| Base listener/filter-chain config | Praxis Operator or deployment owner |
| Praxis Deployment spec and rollout | Praxis Operator or deployment owner |
| Grid overlay `ConfigMap` | Grid Operator |
| Optional generated consumer Praxis `ConfigMap` | Grid Operator |
| Grid TLS Secrets | Grid Operator |
| Mounting/reloading generated config | Deployment owner |

## Current Maturity

**Inference path:** implemented and validated.  `GridNetwork`, `GridSite`, and
`InferenceProvider` have controllers, scoring, overlay rendering, credential
reference projection, and E2E validation coverage.

**MCP / A2A:** architectural direction only.  `AgentToolProvider` and
`AgentToAgentProvider` have CRD schema definitions but no controllers and no
operator-started reconcile loops.

**Known hardening backlog:**

- operator HA and leader election
- SWIM sender/origin binding
- per-`GridNetwork` SWIM/key isolation
- stronger `GridSite` readiness conditions beyond Active
- explicit remote transport contract; no implicit plaintext remote routes
- metrics/health endpoint SSRF and response-size hardening
- `Unreachable` recovery semantics
- unknown cost/health scoring semantics
- gateway loaded-config/status handshake
- CRDT record/tombstone garbage collection
- production deployment hardening
