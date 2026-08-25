# Architecture Overview

AI Grid is the control plane that prepares routing state for Praxis AI
gateways.  It watches Kubernetes resources, learns remote provider state,
scores candidates, and writes a local routing overlay.  The gateway then uses
that overlay on the request path.

The important boundary is simple:

```text
Grid decides what should be routable.
Praxis AI performs the actual request routing.
```

Grid does not proxy HTTP traffic.  It does not parse OpenAI requests, inject
provider credentials, terminate data-plane TLS, or call model backends.  Those
jobs live in Praxis AI and Praxis Core.

## Why Grid Exists

Without Grid, every gateway would need static knowledge of every model backend,
remote cluster, credential placement rule, health signal, and routing fallback.
That does not scale across multi-cluster and mixed-provider environments.

Grid turns that moving control-plane state into a local file that Praxis AI can
route from cheaply:

```text
Grid CRDs + local health + remote SWIM/CRDT state
  → scored routing candidates
  → versioned routing overlay ConfigMap
  → Praxis AI validates and accepts a routing snapshot
  → intelligent_route serves requests from that snapshot
```

The request hot path stays local.  A request should not call Kubernetes, SWIM,
CRDT, or the Grid operator to decide where to go.

## Global Ingress and Provider Boundaries

External ingress uses two independent routing decisions:

```text
external client
  -> managed DNS / Anycast / global traffic manager selects an edge
  -> Praxis AI edge gateway authenticates and parses the request
  -> intelligent_route selects an eligible provider from the local Grid overlay
  -> gateway-to-gateway mTLS
  -> Praxis AI provider gateway authenticates the edge
  -> provider-local route and credential policy
  -> private inference backend
```

The global traffic manager owns the stable public name, edge health, public
traffic steering, and edge withdrawal. Grid does not replace that service.
Grid begins after a request reaches a Praxis edge and selects the provider that
can satisfy the authenticated inference request.

The edge and provider fleets are independently replicated. An east edge may
select a west provider, and a west edge may select an east provider. This
separation keeps model, tenant, provider admission, and capacity policy out of
DNS while allowing the outer traffic-management layer to focus on edge
availability and client-to-edge policy.

The provider gateway is a security boundary rather than a transparent proxy.
It authenticates the edge certificate, validates the selected candidate
against provider-local policy, removes untrusted internal headers, injects any
provider-owned final-hop credential, and forwards only to an authorized private
backend.

See [External Client Ingress](external-ingress.md) for the complete production
contract and the
[Praxis demos repository](https://github.com/praxis-proxy/demos) for automated
runtime proof of the principal flows and failure cases.

## Deployment Topologies

Grid does not require consumer and provider gateways to run in separate
clusters. Operators can choose dedicated gateway clusters for stronger
infrastructure isolation or combined sites when reducing cluster count is more
important. The request and authorization contracts remain the same in both
topologies.

### Dedicated Consumer Or Edge And Provider Clusters

```text
workload or external client
             |
             v
+-----------------------------+
| consumer or edge cluster    |
|                             |
| Praxis consumer/edge gateway|
+-----------------------------+
             |
             | authenticated provider hop
             | gateway-to-gateway mTLS
             v
+-----------------------------+
| provider cluster            |
|                             |
| Praxis provider gateway     |
|             |               |
|             v               |
| private inference endpoint  |
+-----------------------------+
```

- **Security boundary:** Provider credentials, private backend addresses, and
  provider-side authorization remain in a cluster that does not host
  workload-facing gateways. A compromise of the consumer cluster does not by
  itself grant access to provider Secrets or the provider control plane.
- **Failure domains:** Consumer and provider roles use separate Kubernetes
  control planes, nodes, and cluster networks. A cluster outage or failed
  upgrade affects one role without necessarily removing the other role's
  capacity.
- **Network isolation:** Provider clusters can reside on restricted network
  segments that are reachable only through authenticated provider gateways.
  Consumers never require direct connectivity to private inference endpoints.
- **Ownership and compliance:** Different teams can own the consumer and
  provider environments, certificates, policies, audit records, and
  maintenance schedules. Regulated providers can remain outside a general
  workload cluster's administrative boundary.
- **Scaling and operations:** Consumer gateways and provider capacity scale
  independently and can use different node types, quotas, availability goals,
  and rollout schedules. This flexibility adds clusters, certificates,
  network paths, capacity planning, and operational coordination.
- **When to choose it:** Use dedicated clusters when control-plane separation,
  credential isolation, restricted provider reachability, independent failure
  budgets, or a smaller configuration blast radius justify the additional
  infrastructure.

The separated-role workload demo and global-ingress demo provide runtime proof
for dedicated gateway paths.

### Combined Consumer And Provider Sites

```text
local workload
      |
      v
+-----------------------------------+
| combined-site Kubernetes cluster  |
|                                   |
| Praxis consumer gateway           |
|      |                            |
|      | separate identity, policy, |
|      | and Service boundary       |
|      v                            |
| Praxis provider gateway           |
|      |                            |
|      v                            |
| private inference endpoint        |
+-----------------------------------+
      |
      +---- eligible remote provider gateways at peer sites
```

- **Logical separation:** Consumer and provider gateways remain separate
  Deployments and Services with distinct Praxis configuration, TLS identities,
  ServiceAccounts, authorization policies, and Secret mounts. Combining a site
  does not mean combining both roles into one process.
- **Credential isolation:** Provider credentials are mounted only in the
  provider gateway. NetworkPolicy and provider-route authorization prevent the
  consumer gateway and ordinary workloads from bypassing the provider gateway
  to reach the private inference endpoint.
- **Shared failure domain:** Both roles share a Kubernetes control plane,
  cluster network, and potentially the same nodes. A cluster-wide outage,
  administrative compromise, or disruptive maintenance event can affect both
  consumer and provider paths at once.
- **Resource efficiency:** One cluster can supply local workload access and
  local provider capacity, reducing control-plane count and infrastructure
  cost. Separate resource requests, limits, scheduling rules, and disruption
  policies should still prevent one role from starving the other.
- **Operational fit:** Combined sites suit development, compact environments,
  branch locations, and platforms where the same team owns both roles and the
  Kubernetes security boundary satisfies policy requirements.
- **When not to choose it:** Do not consolidate when provider credentials or
  networks must be outside workload-cluster administration, when roles require
  separate compliance domains, or when sharing a cluster creates an
  unacceptable failure or change blast radius.

### Routing And Failover In Either Topology

- **Provider selection:** Grid may select any eligible local or remote provider
  gateway represented in the consumer's accepted routing overlay. Physical
  colocation does not make a provider automatically eligible.
- **Backend boundary:** Consumer and edge gateways route to provider gateways,
  never directly to private inference endpoints. The provider gateway remains
  responsible for peer authentication, exact route authorization, and
  final-hop credential injection.
- **Policy preservation:** Failover must remain within the candidate set
  authorized for the tenant, trust domain, compliance boundary, workload
  class, and provider network. Reachability alone is not authorization.
- **High availability:** Multiple replicas behind one logical gateway provide
  process or node availability. Multiple logical gateways or sites provide a
  separate routing and failure boundary; they are not interchangeable concepts.

## The Stack

Grid sits above the Praxis data plane:

| Layer | Role |
|---|---|
| **Grid Operator** | Kubernetes control plane. Watches Grid CRDs, exchanges provider state, scores candidates, publishes versioned routing overlays, reports rendered and distributed revisions, and manages Grid trust material. |
| **Praxis AI** | AI-aware gateway. Runs request parsing, `intelligent_route`, the AI-owned `X-AI-Routing-*` provider-hop contract, exact `provider_route`, optional `credential_inject`, and AI-specific packaging. |
| **Praxis ExtProc** | Envoy ExternalProcessor service that runs Praxis filter pipelines for deployments that retain Envoy in front of Praxis. |
| **Praxis Core** | Generic proxy/filter runtime. Owns listeners, filter pipelines, load balancing, `endpoint_selector`, `peer_identity_trust`, TLS integration, and request context. |
| **Pingora** | Low-level async proxy engine under Praxis. Handles TCP/TLS, HTTP codecs, connection pooling, and upstream I/O. |

The split keeps Grid focused on state preparation and keeps request handling in
the gateway process that already owns the network hot path.

## Control-Plane Resources

The implemented inference path uses three cluster-scoped CRDs:

| CRD | Current role |
|---|---|
| `GridNetwork` | Defines a logical Grid: SWIM seeds, TLS settings, gateway references, and optional consumer config generation. |
| `GridSite` | Represents one participating site or cluster. Tracks discovery, gateway address, public trust material, fingerprint trust, and phase. |
| `InferenceProvider` | Declares model capacity: model name, backend kind, endpoint, health config, auth strategy, access policy, and provider status. |

`AgentToolProvider` and `AgentToAgentProvider` are schema direction for MCP and
A2A.  `AgentToolProvider` has a running reconciler that resolves `siteSelector`
matches and live-probes the endpoint's MCP `tools/list` contract, but does not
yet distribute discovered tools across sites via SWIM/CRDT, score them, or
render a routed data-plane path — those remain grid-local only.
`AgentToAgentProvider`'s resource type exists, but the operator does not yet
run a controller for it at all.  Inference is the mature reconciled path
today.

See [CRDs](crds.md) for field-level details.

## How a Provider Enters the Grid

A backend becomes routable in stages:

```text
Provider site declares an InferenceProvider
  → local Grid operator validates placement, status, and Secret references
  → local provider state is recorded as CRDT state
  → SWIM carries that state to peer Grid operators
  → peers merge the CRDT state into their local view
  → each operator applies access policy and scoring for its own gateways
  → each operator publishes a scoped, versioned overlay for its own gateways
```

Each operator renders from its own local view of the world:

```text
local Kubernetes CRDs
+ local observed provider/site status
+ remote provider/site state received over SWIM/CRDT
= this operator's local routing view
```

Sites should converge, but they are not guaranteed to have identical views at
every instant.  Overlay rendering is reconcile-driven, not request-driven.

The rendered overlay has a content-addressed revision. The revision covers only
routing-relevant content, so a timestamp or provenance update does not create a
new routing revision. This lets operators correlate what Grid rendered, what
Kubernetes distributed, what Praxis AI accepted, and what a request actually
used.

## SWIM and CRDT State

Grid uses `foca`, a Rust SWIM implementation, for membership gossip.  `foca`
used the Go memberlist model as a reference architecture, but Grid does not use
memberlist itself.

SWIM answers:

```text
Which peer Grid operators are alive?
```

CRDT state answers:

```text
What provider and site state has each peer advertised?
```

Neither SWIM nor CRDT is an authorization engine.  Discovery alone does not make
a site routable.  A provider still has to pass lifecycle, trust, freshness,
placement, and access-policy checks before it enters a gateway overlay.

Important current limitation: SWIM encryption proves membership in the shared
key group, but stronger sender/origin binding is still hardening work.  Do not
treat distributed CRDT state as fully security-sensitive routing input until
that work is complete.

## Single-Site and Combined Deployments

SWIM membership is **site-granular**: each Grid operator is a single SWIM node
carrying its site's identity, and SWIM members are *other sites'* operators - not
the gateways, providers, or pods inside a site. Seeds (`GridNetwork.spec.seeds`)
point at other sites, and the operator filters out its own address, so a lone
site legitimately forms a **single-node mesh with zero peers**.

```text
Multi-site                              Single / combined site
--------------------------------        ------------------------------
site-a operator -- SWIM -- site-b       one operator -- SWIM (self only)
    |                        |              |
 local providers        local providers   several gateways / providers
                                          (NOT SWIM members)
```

Two things that commonly surprise people on a single or combined cluster:

- **Zero SWIM peers is expected, not a failure.** There is no second site to
  discover, so do not add SWIM seeds or extra operator replicas to "make
  discovery work."
- **Local routing does not require `GridSite.status.phase == Active`.** Local
  `InferenceProvider`s are eligible regardless of GridSite phase; only *remote*
  (cross-site CRDT) provider records are phase-gated. So a single-site deployment
  routes to its local providers even while its own `GridSite` is `Pending`.

See the [GridSite lifecycle](crds.md#gridsite) for the phase machine and this
single-site behavior.

## Routing Overlays

For each gateway reference on a `GridNetwork`, the operator writes a
`ConfigMap` with two representations of the same routing state:

- `routing-overlay.json` is the versioned envelope consumed by gateways that
  enforce the observable overlay contract.
- `routing-config.json` is the bare routing payload retained for consumers that
  have not enabled the envelope contract.

The versioned envelope contains:

- a schema version
- a content-addressed semantic revision and matching SHA-256 content digest
- scope binding for the network, gateway, namespace, and local site
- bounded producer and `GridNetwork` provenance
- the routing payload

The routing payload contains:

- the local site name for that gateway
- candidate model/provider entries
- candidate site and cluster identities
- freshness and ordering information
- optional credential references

Credential references contain locating information only:

```text
strategy + Secret name + namespace + key
```

Token bytes are never written into overlays, generated `ConfigMap`s, status, or
logs.

The Grid operator reports the rendered revision and last successfully
distributed revision separately in `GridNetwork.status.overlayStatus`. If an
apply fails, status preserves the last distributed revision while reporting the
new render attempt. A `ConfigMap` annotation exposes the schema, revision, and
digest without requiring an operator to parse its data.

Praxis AI validates the schema, digest, scope, provenance, and candidate bounds
before accepting an envelope. An invalid cold-start overlay prevents the
gateway from becoming ready. An invalid replacement does not displace the
in-memory last-known-good snapshot.

See [Routing](routing.md) for the envelope format, revision semantics, and
regeneration triggers.

## Scoring and Selection

Grid applies one provider-level scoring strategy before writing the overlay.
`noMetrics` is the generic default for external APIs and providers without
comparable telemetry; it gives all admitted candidates the same dynamic score.
llm-d pools can opt into `queueDepth` to prefer the shortest normalized queue
or `kvCachePressure` to prefer the most available KV-cache capacity.
Unavailable providers are excluded, while stale or degraded candidates can
remain as lower-preference fallbacks.

Grid does not perform request-specific prefix scoring. That requires the
current request and per-endpoint cache state, so it remains inside llm-d EPP
after Grid has selected the provider pool.

At request time, Praxis AI `intelligent_route` consumes the loaded overlay. It
does not recompute Grid's score. Its job is to match the requested
model or MCP tool against the already-loaded candidate set and choose the best
candidate under its request-time rules.

See [Scoring](scoring.md) for the full scoring model and known unknown-data
semantics.

## Request Flow

Once the overlay is loaded, a remote-provider request follows two gateway
pipelines:

```text
client request
  → Praxis AI consumer or edge gateway
  → request-format filter extracts model/tool metadata
  → intelligent_route selects a provider gateway from the loaded overlay
  → gateway-to-gateway mTLS
  → Praxis AI provider gateway
  → peer_identity_trust authenticates the calling Grid peer
  → provider_route validates the selected candidate and local route
  → credential_inject adds provider auth for the final backend hop
  → load_balancer selects the authorized local backend
  → response returns to the client
```

When the consumer gateway itself owns the final backend connection, it can run
the final-hop credential and load-balancing stages locally. A remote provider
credential remains at the provider site and is never sent to the edge.

For Chat Completions-style requests, the parser is typically a generic body
field extractor.  For `/v1/responses`, Praxis AI uses
`openai_responses_format` to parse the Responses API shape and promote the model
for `intelligent_route`.

The selected `cluster` is a Praxis load-balancer cluster name.  The overlay can
switch a request from `cluster-east` to `cluster-west` only if both clusters are
already present in the Praxis AI `load_balancer` config.  The overlay does not
create endpoint definitions.

## Credential Flow

Credential handling follows the final-hop rule:

```text
The gateway or provider-side component that makes the final backend call owns
and injects the backend credential.
```

Examples:

| Scenario | Credential lives with | Injector |
|---|---|---|
| Local self-hosted backend | Local/provider site, if needed | Local/provider gateway |
| Remote Grid site | Remote provider site | Remote provider gateway |
| Direct API fallback | Consumer/final-hop site | Consumer gateway |
| Direct Bedrock fallback | Consumer/final-hop site | Consumer gateway or provider-side component authorized for Bedrock |
| Cloud-managed behind provider gateway | Provider site | Provider gateway |
| mTLS-only provider | No HTTP token | None |

Grid validates `InferenceProvider.spec.auth.secretRef` and projects only the
reference into the overlay.  Praxis AI `credential_inject` reads the
mounted Secret file in the gateway that is allowed to call the backend and
injects the outbound header.

Grid does not copy Secret values across clusters.

## ConfigMap Handoff

Rendering a new `ConfigMap` is not enough by itself.  Kubernetes can project the
new file into a pod, but the running gateway still has to consume it.

The recommended production handoff uses the `grid-overlay-sync` container:

```text
Grid operator
  -> applies a scoped, content-addressed overlay ConfigMap
Kubernetes API watch
  -> overlay-sync receives the new resource version
overlay-sync
  -> validates size, schema, scope, revision, and digest
  -> atomically replaces the file in a shared emptyDir
Praxis AI
  -> observes the file change and hot-reloads the accepted snapshot
```

Praxis hot reload is already fast after the file changes. The sidecar exists
because a directly projected ConfigMap is refreshed by the kubelet on an
eventual schedule. Runtime testing observed delays long enough for a temporary
routing preference to change and recover before the gateway saw either update.
Watching the Kubernetes API removes that projected-volume delay from the
normal path. The remaining time is primarily operator scrape/reconciliation
plus API-watch and Praxis file-watch processing; the sidecar does not make the
operator reconcile more frequently.

The current handoff boundary is:

| Owner | Responsibility |
|---|---|
| Grid operator | Render a content-addressed envelope, apply the consumer `ConfigMap`, and report rendered and distributed revisions |
| `grid-overlay-sync` init container | Wait for the first valid operator overlay, validate it, and write it before Praxis starts |
| `grid-overlay-sync` sidecar | Watch one named `ConfigMap`, reject invalid replacements, atomically write valid revisions, retain the last-known-good file, and report delivery health |
| Praxis AI | Strictly validate the projected envelope and atomically replace the accepted in-memory routing snapshot |
| Deployment owner | Configure expected scope, sidecar image, reload policy, and monitoring for distributed, written, accepted, and serving revisions |

This keeps Grid outside the request path and outside the gateway deployment
lifecycle. Grid updates desired routing configuration; Praxis AI can load a
valid update without a pod restart and retains its last-known-good snapshot
when a replacement is invalid. Grid does not restart Praxis pods, and an
applied `ConfigMap` alone is not proof that the gateway accepted its newest
revision.

The sidecar also creates a deliberate security boundary. A dedicated
ServiceAccount receives namespaced `get`, `list`, and `watch` access for the
configured overlay `ConfigMap`. Only the init and sidecar containers mount its
projected token. The Praxis container mounts the resulting overlay directory
read-only and receives no Kubernetes API credential.

Direct ConfigMap projection remains an opt-out compatibility mode. It has fewer
components, but delivery latency is controlled by the kubelet and the handoff
does not expose sidecar validation, last-known-good, or delivery-status metrics.

The revision lifecycle uses four distinct terms:

| Stage | Meaning |
|---|---|
| **Rendered** | Grid produced a valid envelope and semantic revision. |
| **Distributed** | Kubernetes accepted the overlay `ConfigMap`; Grid records its `resourceVersion`. |
| **Accepted** | Praxis AI validated the envelope and installed its immutable in-memory snapshot. |
| **Serving** | A request selected a route from that exact accepted snapshot. |

Praxis AI emits the accepted revision when it loads an overlay. For
provider-bound requests, the edge also carries the serving revision in bounded
provider-hop context. The provider gateway consumes and removes that edge-owned
header, then writes a provider-owned revision header for backend telemetry.
Neither revision header grants authority: mTLS peer identity and
provider-local candidate, model, and path policy remain the authorization
boundary.

When transport configuration changes, such as changing a remote endpoint from
`plaintext` to `mutual_tls` or updating `transport.sni`, the deployment owner
must ensure the gateway reloads that configuration.

## Trust and Readiness

Grid manages control-plane trust material and can generate Grid CA/site
certificates.  It also records public trust material and fingerprint policy for
discovered sites.

`GridSite.status.phase == Active` currently means control-plane eligibility:

```text
the configured fingerprint matched
+ the TCP probe passed
= Grid has enough information to consider the site for overlay generation
```

It does not prove that a Praxis gateway has completed an mTLS handshake,
accepted client identity, loaded the newest overlay, or authorized provider-side
traffic.  Those are data-plane readiness concerns and need richer status
conditions over time.

Over time, readiness should distinguish states such as:

- discovered
- transport reachable
- certificate pinned
- mTLS verified
- peer authorized
- routing config loaded
- routing ready

## External Client Ingress

Grid's baseline path serves in-cluster workloads through a cluster-local
Praxis consumer gateway. External client ingress extends this to clients
outside the cluster: a stable public DNS name backed by global traffic
management routes clients to a healthy Praxis AI edge-ingress gateway, which
uses the same Grid overlay and `intelligent_route` filter to select a provider.

The edge tier is intended to be active-active behind platform-owned traffic
management. Praxis AI is the L7 AI router and data-plane target, not the
complete global traffic-management system.

See [External Client Ingress](external-ingress.md) for the full design,
ownership boundaries, authentication model, and production contract.

## Boundaries to Keep in Mind

Grid is intentionally not the whole platform.  It prepares and publishes routing
state, while Praxis AI, Praxis Core, Kubernetes, and the deployment owner each
own different parts of the running gateway.

The most important boundaries are:

- `GridSite Active` is not the same as end-to-end gateway readiness.
- A rendered or distributed overlay is not the same as a gateway accepting or
  serving that revision.
- Credential references are not credential values.
- SWIM membership is not authorization.
- Inference is the primary routed path; MCP and A2A should be treated as
  separate routed surfaces as their controllers and overlays mature.

When evaluating a new feature, first decide which side owns it:

```text
Does it change provider state, policy, scoring, or overlay content?
  → Grid control plane

Does it change request parsing, route selection, credential injection, or
upstream proxy behavior?
  → Praxis AI / Praxis Core data plane
```
