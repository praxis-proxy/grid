# External Client Ingress

Grid supports two complementary ingress patterns for AI inference routing.

## Ingress patterns

### Workload ingress

An in-cluster workload calls its cluster-local Praxis gateway. That gateway
uses a Grid routing overlay to select a provider site for the requested model.
This is the baseline Grid data-plane path documented in [Routing](routing.md).

```text
in-cluster workload
  -> cluster-local Praxis consumer gateway
  -> Grid-selected provider gateway (local or remote)
  -> inference backend
```

### External client ingress

An external client calls a stable public DNS name. Global traffic management
sends the connection to a healthy Praxis AI edge-ingress gateway. That gateway
uses a Grid routing overlay to select a provider site, the same way a
cluster-local consumer gateway does.

```text
external client
  -> public DNS / managed GLB / Anycast / CDN
  -> Praxis AI edge-ingress gateway
  -> Grid-selected provider gateway
  -> inference backend
```

Both patterns use the same Grid overlay format, the same `grid_route` filter,
and the same provider gateway trust model. The difference is what sits in front
of the Praxis gateway: a cluster-local service for workload ingress, or a
global traffic manager for external client ingress.

## One name, not one process

One stable public DNS name does not require one central gateway process. The
intended production shape is an active-active fleet of Praxis AI edge-ingress
gateways behind platform-owned global traffic management.

```text
one public service name  !=  one public gateway instance
```

The name and API contract are global. The edge execution tier is replicated.
The Grid control plane remains distributed. Provider clusters remain private.
No single Praxis edge instance is required on the live request path for all
traffic.

## Ownership boundaries

External client ingress spans four ownership boundaries:

| Owner | Responsibility |
|---|---|
| **Global traffic manager** | Client-to-edge selection, public DNS, public TLS front door, DDoS/WAF/Anycast/cloud LB behavior, edge health steering. |
| **Grid** | Provider discovery, policy eligibility, scoring, routing overlay generation. One overlay per gateway perspective. |
| **Praxis AI** | Request-time parsing, auth/filter chain execution, `grid_route` selection from a loaded overlay snapshot, forwarding to the selected provider gateway. |
| **Praxis core / Pingora** | Lower-level proxying, TLS termination, connection pooling, load-balancer mechanics. |

Praxis AI is the L7 AI router and GLB data-plane target. It is not the
complete global traffic-management system. DNS health steering, Anycast
advertisement, DDoS protection, and internet-scale traffic absorption are
external platform infrastructure.

Praxis AI must not join SWIM, call Kubernetes, or call the Grid operator in the
request path. It consumes a local validated snapshot rendered by Grid.

## Two-stage routing

External ingress requires two routing decisions because they use different
information available at different times.

### Stage 1: client to edge

The global traffic manager selects a healthy edge based on network proximity,
latency, geography, policy, and edge health. This decision happens before the
HTTP request body is available. The global traffic manager does not know the
requested model, tenant policy, or provider capacity.

### Stage 2: edge to provider

After receiving the request, the Praxis AI edge parses the OpenAI-compatible
body, extracts the model, and invokes `grid_route` against the loaded Grid
overlay. The overlay provides eligible provider candidates ordered for that
edge's routing perspective. Praxis selects a candidate and forwards to the
provider gateway using the existing `load_balancer` / Pingora path.

DNS must not select the provider site because it does not know the requested
model.

### Request flow (target architecture)

```text
external client
  -> TLS to api.example.com
  -> global traffic manager selects healthy edge
  -> Praxis AI edge: [external auth] -> [rate limit] -> parse model
  -> grid_route: in-memory candidate lookup from loaded overlay
  -> Grid mTLS to selected provider gateway
  -> provider gateway: verify edge peer identity, forward to backend
  -> inference backend returns response
```

Steps in brackets are production requirements not yet implemented in the POC.
The request path must not call Kubernetes, Grid operators, SWIM, CRDT, DNS
control APIs, or the filesystem.

### Control-plane flow (target architecture)

```text
provider CRDs + metrics + SWIM/CRDT state
  -> Grid reconciliation
  -> edge-specific routing overlay ConfigMap
  -> projected ConfigMap volume (Kubernetes deployments)
     or overlay-sync adapter (standalone container POC)
  -> atomic file replacement
  -> grid_route validates and atomically swaps the in-memory snapshot
  -> subsequent requests use the new snapshot
```

The last two steps — in-process snapshot validation and atomic swap — depend
on Praxis AI overlay-file hot reload, which is not yet merged.  Until that
lands, overlay updates require a gateway restart.  See
[Current implementation status](#current-implementation-status).

Praxis AI does not become a SWIM member. The Grid operator or a future Grid
edge agent represents the site in the Grid control plane. Praxis remains the
data plane and consumes a local validated snapshot.

## Authentication boundaries

External client ingress introduces a three-layer authentication model. Each
layer serves a different trust boundary and must not be conflated with the
others.

### External caller authentication

The customer's bearer token, JWT, or API key authenticates and authorizes the
external caller at the edge. This is terminated before `grid_route` runs.

The customer's `Authorization` header must not be forwarded as a provider
credential. It must be stripped or replaced before the request leaves the edge.

### Grid mTLS peer identity

The edge gateway's Grid site certificate authenticates the edge to provider
gateways. Provider gateways verify the edge peer identity using
`peer_identity_trust` and reject untrusted peers before forwarding to local
infrastructure.

Public TLS certificates (for `api.example.com`) must be kept separate from Grid
site mTLS certificates. They serve different trust domains and have different
rotation lifecycles.

### Provider credential injection

The provider credential authenticates the final-hop gateway to SaaS or cloud
provider APIs. This is handled by `grid_credential_inject` at the authorized
final-hop point, using a mounted Secret file. Grid carries only credential
references in the overlay, never token values.

See [Authentication & Access Policy](auth.md) for the full credential flow.

### Current gap: tenant and model authorization

Grid's provider `accessPolicy` is site-oriented: it controls which Grid sites
can consume a provider. An edge site's provider eligibility does not authorize
every customer to every model. Production external service requires
request-time tenant-to-model authorization policy that is separate from Grid's
site-level access control. This is not yet implemented.

## Current implementation status

The following reflects the current state of relevant components:

- **Grid routing overlays**: Grid renders per-gateway routing overlays with
  scored candidates. The overlay format and `grid_route` consumption path are
  implemented and validated. See [Routing](routing.md).

- **Grid consumer config**: The operator can generate complete consumer Praxis
  configurations from `GatewayRef` data, including explicit endpoint transport
  and credential references. See [Consumer Config](consumer-config.md).

- **Grid explicit endpoint transport**: The `clusterEndpoints` transport shape
  (`mutual_tls` / `plaintext`) is implemented with fail-closed validation.

- **Grid deployment package**: The `/deploy` directory provides CRDs, operator
  manifests, and RBAC configuration.

- **Forge**: The initial CLI crate supports `doctor`, `plan`, `config`,
  cluster lifecycle (`up`/`down`/`status`/`cluster` subcommands), persistent
  state with locking, Docker/Podman runtime detection, and ownership-safe
  container network create/remove. Service lifecycle, stack execution, and
  full POC environment composition are not yet implemented.

- **`grid_route` overlay mode**: Praxis AI PR #339 introduces static candidate
  mode for `grid_route`. Overlay-file hot reload, which would allow dynamic
  failover without restarting the Praxis edge process, is follow-up work. Do
  not claim dynamic in-process overlay reload until that work lands and is
  proven.

- **Provider credential injection**: Praxis AI `grid_credential_inject` is
  separate from external caller authentication. It handles provider-side
  credential injection at the final hop.

- **Gateway peer identity**: Praxis core `peer_identity_trust` is available
  for provider gateways to verify edge peer certificates.

## POC topology

The first proof-of-concept demonstrates external client ingress in a local
development environment. It is explicitly not a production GLB deployment.

### Components

- **Provider clusters**: Two KIND clusters (e.g. `provider-east`,
  `provider-west`) on a Forge-managed container network, each running Grid
  operator, CRDs, inference simulators, and a Praxis provider gateway.

- **Edge control cluster**: A KIND cluster providing the Grid control-plane
  companion for the edge (operator, CRDs, overlay rendering). The Praxis edge
  process itself remains a standalone container.

- **Praxis AI edge container**: A standalone container connected to the Forge
  network, bound to `127.0.0.1` on the host for the POC. Configured with a
  Grid routing overlay for its edge perspective (e.g. `edge-us-east`). Uses
  Grid mTLS when calling provider gateways.

- **Provider gateways**: Exposed only on the Forge container network (e.g. via
  MetalLB addresses). Not exposed on host ports.

### POC DNS

The POC uses `curl --resolve` to emulate a stable public name without changing
host DNS:

```console
curl --resolve api.grid.test:8443:127.0.0.1 \
  https://api.grid.test:8443/v1/chat/completions \
  -H 'Content-Type: application/json' \
  --data '{"model":"shared-model","messages":[{"role":"user","content":"hello"}]}'
```

This is not a claim that Forge implements production DNS, Anycast, or global
traffic management.

### POC network path

```text
host client
  -> 127.0.0.1:8443
  -> standalone Praxis edge container
  -> Forge container network
  -> provider gateway (MetalLB address on Forge network)
  -> inference backend
```

Provider gateways and inference services do not bind host ports.

### What the POC does not include

- Production DNS, Anycast, CDN, or managed GLB.
- DDoS or WAF protection.
- External customer authentication or tenant isolation.
- Active-active edge fleet (single edge for the initial POC).
- Dynamic overlay hot reload (depends on Praxis AI follow-up work).
- Automatic failover without edge process restart (depends on overlay hot
  reload).

## Production requirements

The following are required for production external service but are not
implemented or proven today. They are listed here as architectural
requirements, not as claims of current capability.

### Active-active edge fleet

Run at least two edge instances in separate failure domains. Each edge
receives its own routing overlay rendered for its location perspective. An
edge must continue serving its loaded overlay during temporary control-plane
disconnection.

### Route-aware edge readiness

Praxis's generic readiness behavior must be reviewed for multi-provider Grid
use. Marking the whole edge unready because any single provider is unavailable
is too strict. A route-aware readiness policy should consider whether at least
one eligible route exists for the public service.

### Overlay freshness and readiness policy

Last-known-good routing is necessary, but serving an old snapshot indefinitely
is not a complete production policy. Production deployments need overlay
revision tracking, reload success/failure counters, configurable readiness
thresholds, and optional hard expiry for environments that must fail closed.

### Public TLS separate from Grid mTLS

Public server certificates for the external endpoint must be separate from Grid
site mTLS certificates. They serve different trust domains.

### External customer authentication and tenant authorization

External callers must be authenticated before `grid_route` runs. Tenant-to-model
authorization must be enforced before routing. Grid's site-oriented
`accessPolicy` does not cover per-tenant model authorization.

### Rate, concurrency, and body limits

Edge gateways need maximum connections, request/response body size limits,
per-client and global rate limits, concurrency limits appropriate for
long-running inference, and upstream timeouts.

### No unsafe automatic POST replay

OpenAI-compatible inference calls are `POST` requests that can consume
expensive capacity even when the client never receives the response. Proxies
must not automatically replay a request after request bytes may have reached a
provider. Safe pre-connect failover (before the upstream request is sent) is
acceptable.

### Streaming drain behavior

SSE inference streams are long-lived requests. Edge shutdown must stop
accepting new traffic and give existing streams time to complete. Active
streams must not be migrated between edges or providers.

### Managed DDoS/WAF/global traffic steering

Praxis should not replace a managed DDoS/WAF/Anycast service. Production
deployment should place the Praxis edge fleet behind the organization's
approved internet edge.

### Observability

Production deployments need request-level metadata including edge site/region,
requested model, selected provider site/cluster, overlay revision and age,
route decision reason, and auth/rate-limit outcomes. Prompts, tokens, API keys,
and provider credentials must not be logged.

## Location-aware affinity

Location is a property of the edge deployment, not a client-controlled routing
header.

Each edge has a configured routing perspective:

```text
edge-us-east:
  site = edge-us-east
  region = us-east
```

The global traffic manager steers the client to the nearest healthy edge. Grid
then orders provider candidates for that edge's perspective, considering
policy eligibility, provider health and capacity, location affinity, cost, and
deterministic tie-breaking.

Per-edge location scoring — where Grid derives site distance from the edge's
`GridSite` region and the provider's `GridSite` region — is not yet
implemented.  Current scoring uses `backendKind` locality, which describes
the provider relative to the declaring operator, not relative to an external
edge.  A scoring correction is required before location affinity is accurate
for external edge deployments.

Location is an affinity, not a hard pin. A healthy cross-region provider must
be preferred over an unavailable same-region provider.

Spoofed client location headers (e.g. `X-Region`, `X-Country`) must not
influence routing. Location trust comes from the edge's own configured
identity and the global traffic manager's selection, not from client-supplied
headers.

## Deterministic failure behavior (target)

The following failure modes describe target behavior for production external
service.  They are not all enforced by the current implementation.

- Unknown model: 404 or OpenAI-compatible error, not random fallback.
- No eligible provider: 503 with bounded error.
- Invalid overlay update: last-known-good remains active.
- Expired overlay: readiness degrades or requests fail according to policy.
- Provider loss: only new requests move; in-flight requests are not replayed.
- All providers unavailable: fail closed with `Retry-After`.

## Related documents

- [Architecture Overview](overview.md)
- [Routing](routing.md)
- [Authentication & Access Policy](auth.md)
- [Consumer Config](consumer-config.md)
- [Scoring](scoring.md)
