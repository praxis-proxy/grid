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
  → grid-config.json ConfigMap
  → Praxis AI grid_route
```

The request hot path stays local.  A request should not call Kubernetes, SWIM,
CRDT, or the Grid operator to decide where to go.

## The Stack

Grid sits above the Praxis data plane:

| Layer | Role |
|---|---|
| **Grid Operator** | Kubernetes control plane. Watches Grid CRDs, exchanges provider state, scores candidates, renders `grid-config.json`, and manages Grid trust material. |
| **Praxis AI** | AI-aware gateway. Runs request parsing, `grid_route`, optional `grid_credential_inject`, llm-d/ext_proc support, and AI-specific packaging. |
| **Praxis Core** | Generic proxy/filter runtime. Owns listeners, filter pipelines, load balancing, `endpoint_selector`, `ext_proc`, `peer_identity_trust`, TLS integration, and request context. |
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
A2A.  Their resource types exist, but the operator does not currently run full
controllers, distribute their state, score them, or render complete routed paths
for them.  Inference is the mature reconciled path today.

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
  → each operator renders its own grid-config.json overlay
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

## Routing Overlays

For each gateway reference on a `GridNetwork`, the operator writes a
`ConfigMap` with a `grid-config.json` key.

The overlay contains:

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

See [Routing](routing.md) for the overlay format and regeneration triggers.

## Scoring and Selection

Grid scores provider candidates before writing the overlay.  The current scoring
signals are:

| Signal | Weight | Typical source |
|---|---:|---|
| Locality | 3.0 | Backend kind, site, region |
| Queue depth | 3.0 | Metrics scrape or CRDT state |
| KV-cache utilization | 2.0 | Metrics scrape or CRDT state |
| Prefix-cache hit ratio | 2.0 | Metrics scrape or CRDT state |
| Latency | 2.0 | Metrics or local observation |
| Cost | 1.0 | Provider config |

Higher-scored candidates sort earlier.  `Unavailable` providers are excluded.
Stale or degraded candidates can remain in the overlay as lower-preference
fallbacks.

At request time, Praxis AI `grid_route` consumes the loaded overlay.  It does
not recompute Grid's full scoring formula.  Its job is to match the requested
model or MCP tool against the already-loaded candidate set and choose the best
candidate under its request-time rules.

See [Scoring](scoring.md) for the full scoring model and known unknown-data
semantics.

## Request Flow

Once the overlay is loaded, traffic follows the gateway pipeline:

```text
client request
  → Praxis AI consumer gateway
  → request-format filter extracts model/tool metadata
  → grid_route selects a candidate cluster from the loaded overlay
  → optional grid_credential_inject adds provider auth at the final hop
  → load_balancer selects an endpoint inside the chosen cluster
  → Pingora sends the request upstream
  → response returns to the client
```

For Chat Completions-style requests, the parser is typically a generic body
field extractor.  For `/v1/responses`, Praxis AI uses
`openai_responses_format` to parse the Responses API shape and promote the model
for `grid_route`.

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
reference into the overlay.  Praxis AI `grid_credential_inject` reads the
mounted Secret file in the gateway that is allowed to call the backend and
injects the outbound header.

Grid does not copy Secret values across clusters.

## ConfigMap Handoff

Rendering a new `ConfigMap` is not enough by itself.  Kubernetes can project the
new file into a pod, but the running gateway still has to consume it.

The current handoff boundary is:

| Owner | Responsibility |
|---|---|
| Grid operator | Render and apply the consumer `ConfigMap` |
| Kubernetes | Project the updated `ConfigMap` into the Praxis AI pod filesystem |
| Deployment owner | Restart, roll out, or otherwise reload the gateway so it consumes the updated config |

This keeps Grid outside the request path and outside the gateway deployment
lifecycle.  Grid updates desired routing configuration; it does not restart
Praxis pods or prove the gateway has loaded the latest file.

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

## Boundaries to Keep in Mind

Grid is intentionally not the whole platform.  It prepares and publishes routing
state, while Praxis AI, Praxis Core, Kubernetes, and the deployment owner each
own different parts of the running gateway.

The most important boundaries are:

- `GridSite Active` is not the same as end-to-end gateway readiness.
- A rendered overlay is not the same as a gateway running that overlay.
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
