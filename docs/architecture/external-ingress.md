# External Client Ingress

External client ingress extends Grid's workload-routing model to a stable
public endpoint. A global traffic manager selects an edge. Grid and Praxis then
select and reach an eligible provider.

This document defines the production architecture contract. The
`Repository Implementation` section identifies the capabilities implemented
by the current Grid/Praxis integration. Deployment-specific development
topologies and proof commands are documented with their environment rather
than embedded in this contract.

The architecture has two independent routing stages:

```text
external client
  -> managed DNS / Anycast / global traffic manager
  -> Praxis AI edge gateway
  -> Grid-selected Praxis provider gateway
  -> provider-local inference backend
```

The first stage is network and edge selection. The second stage is
model/provider selection. Combining them would put model, tenant, and provider
capacity policy into DNS, which does not have the authenticated request context
or parsed request body.

## Ingress Patterns

Grid uses the same provider-routing contract for two entry patterns.

### Workload Ingress

```text
in-cluster workload
  -> cluster-local Praxis consumer gateway
  -> Grid-selected provider gateway
  -> inference backend
```

### External Client Ingress

```text
external client
  -> stable public service name
  -> healthy active edge selected by GTM
  -> Grid-selected provider gateway
  -> inference backend
```

The edge fleet is replicated. One public name does not imply one gateway
process or a single Grid controller.

## Component Ownership

| Component | Owned behavior |
|---|---|
| Global traffic manager | Public DNS/Anycast, client-to-edge proximity and latency steering, edge health withdrawal, controlled failback, public-edge DDoS/WAF integration. |
| Grid operator | Site/provider discovery, policy eligibility, provider and metric state, edge-perspective scoring, admission state, ordered overlay generation. |
| Overlay distribution | Delivery of a versioned local snapshot to the edge without entering the request path. |
| Praxis AI edge | External identity/policy filters, model extraction, `grid_route`, session binding, selected-cluster metadata, provider credential injection when the edge is the final hop. |
| Praxis provider gateway | Edge-peer authentication, destination-side authorization, provider-local limits/policy, and private backend forwarding. |
| Praxis core / Pingora | Listener TLS, mTLS, peer identity extraction, connection pooling, health checks, load balancing, timeouts, graceful drain, and upstream I/O. |

Grid is a routing control plane. It does not proxy inference traffic. Praxis AI
does not join SWIM or query Kubernetes, Grid operators, DNS control APIs, or
the filesystem while processing a request.

## Request Path

The external edge pipeline is ordered so hard security and policy decisions
run before provider preference:

```text
request ID and trusted forwarding metadata
  -> external caller authentication
  -> tenant/model/region authorization
  -> rate, concurrency, and body limits
  -> model or capability extraction
  -> optional logical-model classification
  -> Grid candidate matching
  -> established-session binding or new-session selection
  -> provider cluster selection
  -> gateway-to-gateway mTLS
```

The provider gateway authenticates the edge again and applies its own local
authorization. Discovery at the edge does not authorize access at the
destination.

The selected overlay `cluster` names a pre-authorized Praxis load-balancer
cluster. Overlay updates can change candidate eligibility and ordering. They
do not create arbitrary endpoints, CA roots, client keys, or SNI values on the
request path.

## Control-Plane Path

```text
local provider CRDs and observed metrics
  + remote authenticated SWIM/CRDT state
  + GridSite lifecycle and access policy
  -> per-edge eligibility, admission, locality, and scoring
  -> versioned routing overlay ConfigMap
  -> projected Kubernetes volume
  -> strict validation and atomic in-memory swap in Praxis AI
  -> loaded revision exposed by gateway status
```

The edge serves an accepted last-known-good snapshot during a bounded
control-plane interruption. A rejected update never replaces the accepted
snapshot. Overlay age and public route coverage feed edge readiness.

## Routing Order

Hard gates precede preferences:

1. authenticated tenant and model authorization;
2. capability availability and destination trust;
3. residency, external-egress, and sovereignty policy;
4. provider/site lifecycle, health freshness, and admission;
5. logical route class for configured aliases;
6. established-session binding;
7. new-session locality tier;
8. Grid selection tier and rank;
9. deterministic selection among candidates Grid marks equivalent.

Location is a property of the edge's trusted deployment identity. Client
headers such as `X-Region`, `X-Country`, or internal route-class headers do not
control Grid locality.

Grid computes site distance from the edge `GridSite` and provider `GridSite`:

```text
same_site -> same_zone -> same_region -> cross_region -> unknown
```

A closer provider is preferred only while it remains eligible for the request
class. Hard failure, policy loss, stale state, or closed admission overrides
locality.

## Provider Admission

The overlay carries a bounded admission result rather than raw metric series:

| State | New sessions | Established sessions |
|---|---|---|
| `new_and_existing` | allowed | allowed |
| `existing_only` | denied | allowed while the binding remains valid |
| `none` | denied | denied; the binding is replaced or the request fails |

The provider site owns normalized, timestamped capacity signals. Grid owns the
policy that turns those signals into admission state, including hysteresis,
hold-down, expiry, and explicit drain overrides. Praxis consumes the result and
does not reproduce Grid's metric formula.

Unknown, pending, stale, or expired provider state is closed to new work in the
external edge profile.

## Session Affinity

Affinity is keyed from authenticated tenant identity, a validated session ID,
the normalized capability, and any bound route class. Source IP and
client-supplied tenant metadata are not affinity inputs.

For a new session, Praxis selects within the closest usable Grid selection
tier. For an established session, the binding remains authoritative while the
candidate is eligible for existing work. A `none` candidate, provider loss,
policy loss, or capability loss breaks the binding. Recovery does not
automatically pull a rebound session back.

An in-memory binding store scopes affinity to one edge process. Active-active
production uses an explicitly supported shared-store or signed-token contract
with bounded TTL, entry count, creation rate, concurrency behavior, and failure
semantics.

## Authentication Boundaries

External ingress has three separate credential domains.

### Customer Identity

The edge authenticates the external bearer token, JWT, or API key and derives a
bounded tenant/principal context. The external client's `Authorization` header is
removed before gateway-to-gateway or provider traffic.

### Grid Peer Identity

The edge presents its Grid client certificate to a provider Praxis gateway.
The provider validates the CA chain, SNI/server identity, client certificate,
and configured `peer_identity_trust` policy. Public server certificates and
Grid site certificates remain separate trust domains with separate rotation.

### Provider Credential

The component making the final provider API call owns the provider credential.
Grid carries a Secret reference, never credential bytes.

| Route | Credential placement |
|---|---|
| Self-hosted backend through a provider site | Provider site, if required |
| Remote cloud/API provider through a provider gateway | Provider site |
| Direct API fallback from the external edge | Edge, because it is the final hop |
| mTLS-only backend | No HTTP provider credential |

Praxis AI `grid_credential_inject` maps an authorized selected candidate to a
mounted Secret file at the final hop.

## Edge Health and GTM

GTM probes edge liveness and readiness separately:

| Signal | Meaning |
|---|---|
| Liveness | Process and public listener are running. |
| Readiness | Public TLS/auth dependencies work, an accepted overlay is within policy age, and the advertised service has minimum authorized route coverage. |
| Drain | The edge accepts no new connections while existing streams receive a bounded completion window. |

An individual provider failure does not make an otherwise useful edge unready.
Grid removes that provider from eligible routes. Loss of all required route
coverage, a hard-expired snapshot, or a failed security dependency makes the
edge unready and causes GTM withdrawal.

GTM steering moves new connections. It does not migrate an in-flight SSE
stream.

Route-aware readiness belongs with the accepted Grid routing snapshot because
generic process or cluster health cannot prove that the edge has fresh, usable
public route coverage. The traffic manager consumes readiness but never reads
the Grid overlay or selects a provider.

## Retry and Streaming Rules

Inference calls are commonly non-idempotent `POST` requests. Praxis may fail
over before sending request bytes upstream. It does not automatically replay a
request after bytes may have reached a provider unless that API and provider
have an explicit idempotency contract.

During shutdown:

1. readiness is withdrawn;
2. new requests stop;
3. active streams receive the configured drain interval;
4. remaining connections close at the documented hard deadline.

Provider or edge failover applies to later requests, not an active stream.

## Overlay Contract

The production acceptance contract uses a versioned, bounded envelope. The
current repository emits the subset listed under `Repository Implementation`;
revision, digest, expiry, and serving-status fields remain release gates until
their implementation is present across Grid generation, distribution, and
Praxis acceptance.

```json
{
  "api_version": "grid.praxis-proxy.io/v1alpha2",
  "grid_id": "grid-identity",
  "network": "production",
  "consumer": {
    "gateway": "public-edge",
    "site": "edge-us-east"
  },
  "revision": 42,
  "content_digest": "sha256:...",
  "generated_at": "2026-07-25T12:00:00Z",
  "valid_until": "2026-07-25T12:05:00Z",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "shared-model",
      "site": "provider-us-east",
      "cluster": "gateway-provider-us-east",
      "stable_id": "sha256:...",
      "admission_state": "new_and_existing",
      "selection_tier": "same_region",
      "rank": 0
    }
  ]
}
```

The accepted schema defines maximum document size, candidate count, identifier
length, duplicate behavior, supported versions, and unknown-field behavior.
Praxis reports the loaded revision/digest, acceptance time, snapshot age, and
last rejection reason.

## Observability

Every request decision records bounded internal fields:

- edge site and region;
- authenticated tenant class, without raw identity;
- requested logical/concrete model;
- selected provider site, cluster, admission state, tier, and rank;
- overlay revision, digest, and age;
- decision reason and binding outcome;
- caller-auth, authorization, quota, and rate-limit result; and
- upstream connection phase, without request replay.

Prompts, session IDs, raw tenant identifiers, external-client credentials, provider
credentials, private keys, and affinity secrets are never log or metric labels.

Control-plane status distinguishes:

```text
desired -> rendered -> distributed -> accepted -> serving
```

This makes ConfigMap apply success distinguishable from the gateway actually
using that revision.

## Repository Implementation

The Grid and Praxis integration provides:

- per-`GatewayRef` routing overlay generation;
- candidate model/site/cluster identity;
- provider access-policy filtering;
- operator-side scoring;
- geography-derived locality tiers;
- threshold-derived admission metadata;
- `stable_id`, `rank`, and `generated_at` metadata;
- explicit `mutual_tls` or `plaintext` endpoint transport in generated
  consumer config;
- SWIM membership and CRDT provider propagation;
- provider Service address discovery and remote `GridSite` materialization;
- Kubernetes-native provider gateway address discovery;
- edge-local overlay ConfigMaps projected directly into Praxis pods; and
- independently rendered overlays for each edge `GatewayRef`.

Praxis core implements the generic primitives used by the completed path:
upstream mTLS, downstream mTLS identity, `peer_identity_trust`, listeners,
load balancing, health checks, configuration reload, and connection handling.

Praxis AI owns `grid_route`, provider credential injection, AI request parsing,
`grid_provider_route`, and the Grid-specific request-time selection contract.
A compatible Praxis AI build provides overlay-file reload and the configured
session-affinity behavior.

The deployment contract treats a ConfigMap write and process liveness as
control-plane observations, not serving proof. Operators use gateway status,
snapshot age, route coverage, and request evidence to determine whether an
edge is ready for global traffic.

## Provider Boundary

The `provider-gateway` Service selects only Praxis AI provider pods. Provider
listeners require an edge certificate, validate the authorized edge identity
with `peer_identity_trust` as the first unconditional filter, parse the
inference model, and use an exact provider-local candidate/model/path map
before forwarding to a private backend. That map selects a provider-local
Secret reference; `grid_credential_inject` reads the mounted Secret and
replaces the external-client credential only on the final backend hop. The edge
presents its client identity and verifies the provider CA and site-specific
SNI.

`grid_route.provider_hop_clusters` defines the AI-owned serialization
boundary. The filter always removes client-supplied `X-Grid-Peer-*` fields and
reconstructs the selected stable candidate and hop request ID only when the
selected cluster is explicitly allowlisted as a provider hop. Direct backend
and API clusters remain absent from that allowlist. Praxis does not interpret
these fields. The provider consumes them only after mTLS and
`peer_identity_trust`;
`grid_provider_route` then removes them, performs an exact local lookup, and
writes provider-owned backend attribution. `X-Praxis-*` is not used for this
wire contract because Praxis strips its reserved namespace before upstream
requests.

Gateway addresses and provider state use independent SWIM broadcast lanes. A
gateway-only update is accepted independently of the last provider-state
revision, so a Service port or address change cannot remain hidden behind a
higher, unrelated CRDT revision. The receiving operator reconciles the updated
address into the auto-discovered `GridSite.spec.egress.address`.

Provider backends are private Services. Network policy admits inference
traffic from provider-gateway workloads and explicitly authorized health
observers; other workloads are denied. Health observers do not possess the
provider credential.
