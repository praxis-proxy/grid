# Praxis Grid Global Ingress Demo

This environment demonstrates one stable HTTPS inference endpoint backed by
two active Praxis edge gateways and two private Praxis provider gateways. The
east provider cluster hosts two independent providers for the same model, so
the three provider candidates also prove that Grid does not assume one
provider per cluster. Grid discovers provider capacity, renders a local
routing overlay for each edge, and updates the running edge without restarting
it.

New to Grid or Praxis? Start with the
[Architecture Overview](../../docs/architecture/overview.md) for component
ownership and the
[External Client Ingress](../../docs/architecture/external-ingress.md) design
for the production routing and security contract.

The architecture overview's
[Deployment Topologies](../../docs/architecture/overview.md#deployment-topologies)
section compares the dedicated edge and provider clusters used by this demo
with combined consumer/provider sites, including their security,
failure-domain, and operational tradeoffs.

## What The Grid Demo Shows

- **Praxis is a configurable proxy runtime, not one fixed appliance.** The same
  Praxis AI image serves as the global-ingress emulator, public inference edge,
  and private provider gateway. Its listener and filter configuration defines
  the role.
- **Grid is the control plane; Praxis is the data plane.** Grid operators
  discover and score providers, then write edge-local routing overlays. Praxis
  handles requests from an in-memory view without calling Kubernetes, SWIM, or
  the operator on the request path.
- **Edge selection and provider selection solve different problems.** A global
  traffic manager chooses a healthy public edge. After parsing the request,
  `intelligent_route` chooses an eligible inference provider.
- **A cluster can host more than one provider.** The east provider cluster
  advertises two independent `InferenceProvider` resources for the same model.
  Each has its own routing identity, backend, metrics endpoint, and credential,
  while both use the east site's authenticated provider gateway.
- **Security is enforced again at the provider.** Reaching an edge does not
  grant backend access. The provider gateway authenticates the edge identity,
  validates the selected route, owns the final-hop credential, and protects the
  private backend.
- **Sessions can remain stable while capacity changes.** Edge affinity and
  provider affinity use separate keys. Admission can stop new provider sessions
  without breaking an established binding.
- **Routing state can change without replacing the proxy.** Kubernetes projects
  an operator-rendered overlay into the edge pod, and Praxis validates and
  reloads it while the pod UID and restart count remain unchanged.

The narrated demo is organized around five scenarios:

1. active/active global routing with independent Grid provider selection;
2. a secure provider boundary with mTLS, peer authorization, private backend
   policy, and final-hop credential replacement;
3. two-layer session affinity with metrics-driven provider drain and overlay
   hot reload;
4. edge withdrawal, recovery, and failback behind one HTTPS name; and
5. sequential Grid operator restarts followed by a configured request soak.

Every reported `PASS` is based on a runtime assertion. A manifest expressing
intent does not count as proof.

## Client Inference And Workload Inference

Grid supports two entry patterns. This demo focuses on **client inference**:
requests originate outside the Grid, enter through a stable public endpoint,
and pass through a Praxis edge before Grid selects a provider.

### Client Inference: Demonstrated Here

**User story:** As an application owner, I need one stable inference endpoint
that can use healthy edge gateways and eligible providers across sites without
exposing private provider gateways or backends.

```mermaid
flowchart LR
    Client[External inference client]
    GTM[Global traffic management]
    EastEdge[East Praxis edge]
    WestEdge[West Praxis edge]
    Grid[Grid provider selection]
    Providers[Eligible provider gateways]

    Client --> GTM
    GTM --> EastEdge
    GTM --> WestEdge
    EastEdge --> Grid
    WestEdge --> Grid
    Grid --> Providers
```

The local GTM emulator represents the global-ingress layer. It chooses a
healthy edge and provides edge-session affinity. The selected edge then uses
its Grid overlay to make a separate provider decision. The verifier exercises
this complete path, including crossed routes, provider drain, edge withdrawal,
and recovery.

The `east` and `west` names make independent sites and failure domains easy to
see. They do **not** mean that this demo performs geographic client steering,
latency-based edge selection, same-region provider preference, or hard region
locking. The GTM emulator uses health and a deterministic demo affinity key,
and all four edge-to-provider combinations remain eligible.

### Workload Inference

**User story:** As a platform workload, I need to submit inference through my
cluster's Praxis consumer gateway and let Grid select an eligible local or remote
provider without first traversing public global ingress.

See the [Workload Inference Demo](../grid-workload-inference/README.md) for an
automated walkthrough that deploys a 4-cluster topology and proves the in-cluster
request path with runtime assertions. Run it with:

```bash
cargo xtask env run-grid-glb-demo \
  --forge-config demos/grid-glb-demo/forge.yaml \
  --no-ingress --quick --teardown
```

### Regional Policy Status

- **Available as routing input:** `GridSite` carries region and zone metadata,
  and Grid can render locality tiers such as `same_region` and `cross_region`.
  Provider access policy can also limit which consumer sites may use a
  provider.
- **Not proved by this demo:** the verifier does not assert geographic edge
  selection, locality-preferred provider selection, network latency steering,
  or a regional failover policy.
- **Not yet a route-level guarantee:** a request cannot currently declare or
  inherit a trusted policy that means "never leave this region" and fail closed
  when no provider in that region is eligible. That requires an authenticated
  route, tenant, or workload policy contract and explicit allowed-region
  enforcement at provider selection.

## Architecture At A Glance

The demo creates five Kind clusters. Every Praxis process runs in Kubernetes.

```text
Inference client
  |
  | HTTPS: api.grid-glb.test
  v
+---------------------------------------------------------+
| Praxis GTM emulator                                     |
| Kind cluster: grid-glb-gtm-emulator                     |
| Selects a healthy edge; it does not select providers.   |
+----------------------------+----------------------------+
                             |
                 +-----------+-----------+
                 |                       |
                 v                       v
+----------------------------+  +----------------------------+
| Praxis east edge           |  | Praxis west edge           |
| Kind cluster: east-edge    |  | Kind cluster: west-edge    |
| intelligent_route          |  | intelligent_route          |
| east-local Grid overlay    |  | west-local Grid overlay    |
+----------------------------+  +----------------------------+
                 |                       |
                 +-----------+-----------+
                             |
      Each edge prefers eligible local provider candidates,
        but can select the remote gateway for failover.
                             |
                 +-----------+-----------+
                 |                       |
                 v                       v
+----------------------------+  +----------------------------+
| Praxis east provider       |  | Praxis west provider       |
| Kind cluster:              |  | Kind cluster:              |
| east-provider              |  | west-provider              |
+-------------+--------------+  +-------------+--------------+

Provider endpoint mappings:

  east-provider gateway
    +-- simulated provider A: sim-east-provider
    `-- simulated provider B: sim-east-provider-secondary

  west-provider gateway
    `-- simulated provider A: sim-west-provider
```

### Praxis Roles In The Grid Path

| Praxis role | Request-path responsibility | Grid participation |
|---|---|---|
| GTM emulator | Terminates the demo's public TLS connection, checks edge health, and selects an edge using `X-Edge-Session-Id` | None; it selects edges rather than providers |
| Edge gateway | Parses the inference request, applies edge policy, runs `intelligent_route`, and opens the authenticated provider hop | Consumes its edge-local overlay |
| Provider gateway | Authenticates the edge, validates the candidate/model/path, injects the provider credential, and proxies to the private backend | Advertises provider capacity through its local operator |

This role separation comes from configuration and deployment boundaries, not
three unrelated proxy implementations. Praxis Core supplies the listener,
TLS, load-balancing, and proxy runtime; Praxis AI supplies the inference-aware
filters; Grid prepares the provider state those filters consume.

### Choosing A Deployment Shape

The five-cluster layout makes every routing and security boundary visible, but
it is not the minimum deployment shape. A smaller installation can run the
client-facing edge pipeline and the final-hop provider pipeline in one Praxis
deployment:

```text
client
  -> Praxis edge and provider pipelines
  -> local backend or external model API
  -> client
```

That shape is useful for one cluster, one administrative domain, or a compact
development installation. Grid can still prepare provider candidates, and
Praxis can still parse requests, apply policy, select a backend, and inject a
final-hop credential.

The hierarchical edge-to-provider topology becomes important when the platform
needs stronger separation. Collapsing the roles gives up several properties:

- **Independent trust boundary:** there is no distinct gateway-to-gateway mTLS
  hop where the provider can authenticate and authorize the calling edge.
- **Failure and scaling isolation:** edge traffic and backend/provider traffic
  share a deployment lifecycle, resource envelope, and failure domain.
- **Provider-owned credentials and policy:** credentials can remain local, but
  they are no longer isolated behind a separately administered provider
  gateway with its own route and peer policy.
- **Independent provider-site behavior:** a fully collapsed topology has no
  distinct provider site to withdraw, scale, or fail independently. Remote
  provider choice and crossed-region failover require a separately reachable
  provider endpoint.

The right shape depends on ownership and failure boundaries, not merely cluster
count. Start compact when one team owns the entire path; introduce provider
gateways and the hierarchical peer-to-peer path when sites, credentials,
capacity, or policy need independent control.

| Cluster | Praxis workload | Grid role |
|---|---|---|
| `gtm-emulator` | GTM emulator | None; local stand-in for managed global ingress |
| `east-edge` | Public edge gateway | Consumes the east edge's operator-rendered overlay |
| `east-provider` | Private provider gateway | Advertises two independent east inference providers |
| `west-edge` | Public edge gateway | Consumes the west edge's operator-rendered overlay |
| `west-provider` | Private provider gateway | Advertises west inference capacity |

The four edge/provider clusters each run a Grid operator and participate in
one SWIM mesh. The GTM emulator is deliberately outside that mesh because it
selects an edge, not an inference provider.

### Demo Infrastructure: How The Clusters Connect

Forge attaches all five Kind clusters to one dedicated Docker bridge network.
It gives each cluster a non-overlapping MetalLB address pool from that shared
network. The edge and GTM entry points are ordinary Kubernetes
`LoadBalancer` Services:

```text
client
  -> GTM MetalLB VIP:8443
  -> Service/gtm-emulator
  -> GTM Praxis pod
  -> edge MetalLB VIP:8080
  -> Service/edge-gateway
  -> edge Praxis pod
```

After each edge Service becomes ready, Forge captures
`.status.loadBalancer.ingress[0].ip`. Those two captured VIPs, with port
`8080`, become the Praxis GTM emulator's upstream endpoints. Traffic reaches
the selected VIP across the shared Docker network; Kubernetes Service routing
then sends it to the edge pod's `http` target port.

The GTM configuration does not address a NodePort, `hostPort`, host-network
socket, or `kubectl port-forward`. Kubernetes may allocate implementation
NodePorts for a `LoadBalancer` Service, but this demo neither discovers nor
uses them. The client similarly reaches the GTM emulator through its captured
MetalLB VIP, using `curl --resolve` to bind `api.grid-glb.test` to that address.

The GTM-to-edge hop is plaintext HTTP only inside this isolated demo network.
A production traffic manager should use authenticated TLS or mTLS to each
Praxis edge origin.

## End-to-End Request Path

The outer traffic-management decision and the inner Grid decision are
independent. In this example, the stable endpoint sends the client to the east
edge because that edge is healthy and matches the client's edge-affinity key.
After the request reaches that edge, `intelligent_route` independently matches the
requested model and selects from the eligible provider candidates and ranks in
the east edge's loaded overlay.

The demo first proves that the east edge's overlay contains both east provider
identities with distinct stable IDs and can route requests to both private
backends. It then actively proves one reason provider choice changes: a high queue metric
causes the operator to mark the initially selected provider
`existing_only`. The bound provider session remains there, while a new provider
session selects the other fully admitted provider. It separately proves
provider health withdrawal and provider-side peer, model, and path policy. It
does not claim to exercise geographic, latency, cost, or every possible
capacity signal. The resulting crossed path can be:

```mermaid
sequenceDiagram
    participant Client
    participant GTM as Praxis GTM emulator
    participant Edge as East Praxis edge
    participant Provider as West Praxis provider
    participant Backend as West private backend

    Client->>GTM: HTTPS request to api.grid-glb.test
    GTM->>Edge: Select healthy edge using edge affinity
    Edge->>Edge: Parse request and select west provider from local overlay
    Edge->>Provider: Verified mTLS hop with authenticated Grid context
    Provider->>Provider: Authenticate edge and validate candidate, model, and path
    Provider->>Provider: Replace client credential with provider-local credential
    Provider->>Backend: Authorized inference request
    Backend-->>Provider: Inference response
    Provider-->>Edge: Attributed provider response
    Edge-->>GTM: OpenAI-compatible response
    GTM-->>Client: HTTPS response
```

The response attribution identifies the selected edge, provider gateway, and
backend. This lets the narration prove the observed live path rather than infer
it from manifests or configured endpoints.

## Production Architecture And Local Emulation

### Production Global Traffic Management

A production deployment places a managed global traffic-management service in
front of the Praxis edges. Depending on the platform, that may be authoritative
DNS with health steering, an Anycast service, a cloud global load balancer, or
an enterprise GTM product.

That production component owns:

- the stable public hostname and certificate boundary;
- edge health evaluation;
- geographic, latency, policy, or capacity-based edge selection;
- edge withdrawal and recovery;
- public-network protections such as WAF and DDoS controls; and
- the public availability objective for the global endpoint.

Grid begins after an edge is selected. Grid discovers inference providers and
selects the provider gateway that should serve the request.

### Why The Demo Has A GTM Emulator

Kind does not provide managed DNS, Anycast, or a cloud global load balancer.
The `gtm-emulator` cluster runs a small Praxis configuration that represents
that missing outer layer. It provides one verified HTTPS name, health-checks
the two edge endpoints, and uses `X-Edge-Session-Id` for deterministic edge
selection.

```text
Production                         Local demo

managed DNS / Anycast / GLB        Praxis GTM emulator
          |                                  |
   east and west edges              east and west edge Services
```

The emulator proves the contract between a stable client endpoint and two
edges. It does not claim to reproduce Internet routing, DNS propagation,
Anycast convergence, geo-latency measurement, WAF, or DDoS behavior.

The fifth cluster gives the emulator its own Kubernetes lifecycle and failure
domain. It is not a second Grid operator and does not distribute overlays.

### How A Request Reaches An Edge

The GTM emulator does not generate inference requests. A client sends the
request to the stable HTTPS endpoint, and the emulator proxies that request to
one healthy edge:

```mermaid
flowchart TD
    Client[Client] -->|HTTPS api.grid-glb.test:8443| GTM[Praxis GTM emulator]
    GTM -->|Health and edge affinity select an edge| VIP[Selected edge MetalLB VIP:8080]
    VIP --> Service[Service/edge-gateway]
    Service -->|Kubernetes Service routing| Edge[Praxis edge pod]
    Edge -->|intelligent_route reads the edge-local overlay| Provider[Provider gateway:8443 over verified mTLS]
```

Forge captures the MetalLB addresses of the `edge-gateway` Services after both
edge clusters are ready. Those addresses become the two upstream endpoints in
the GTM emulator's Praxis configuration. MetalLB makes each Service address
reachable across the shared local cluster network, and Kubernetes routes the
selected connection to the Praxis pod in that edge cluster.

`X-Edge-Session-Id` influences only GTM edge selection.
`X-Session-Id` influences the later Grid provider selection. Keeping these
keys separate demonstrates that selection of an edge and selection of an
inference provider are independent decisions.

The local emulator terminates public TLS and uses plaintext origin traffic to
the edge Services. A production managed GTM or global load balancer normally
uses authenticated TLS or mTLS to each edge origin.

### GTM Emulator Runtime Assertions

| Scenario | Description |
|---|---|
| Forge configuration | Validates the resolved Forge environment before making runtime claims. The assertion covers the five declared clusters, referenced stacks, rendered manifest inputs, and image configuration required to deploy the GTM, edge, and provider paths. |
| Praxis workloads | Requires exactly one ready Praxis role in each demo cluster: the GTM emulator, east edge, west edge, east provider gateway, and west provider gateway. Readiness comes from the live Kubernetes Deployments rather than Forge host-service state. |
| Edge overlays | Reads the operator-generated routing ConfigMap in both edge clusters. Each document must contain provider candidates and identify its own edge through `local_site`, proving that the edges consume distinct local Grid perspectives rather than one copied overlay. |
| Stable HTTPS endpoint | Sends an inference request through `https://api.grid-glb.test:8443` and requires a successful attributed response. This proves the client-facing certificate and hostname, GTM proxy path, selected edge, Grid provider path, and backend response under one stable entry point. |
| Two edge identities | Searches a bounded set of `X-Edge-Session-Id` values and requires live responses attributed to both `east-edge` and `west-edge`. This proves both edges are active members of the GTM endpoint set rather than a configured but unused standby. |
| Edge session stickiness | Repeats requests for independently discovered session keys mapped to east and west. Each key must remain on its original healthy edge across repeated requests, demonstrating deterministic GTM-layer affinity independently of Grid provider affinity. |
| Edge withdrawal and recovery | Scales the east edge Deployment to zero, waits for the east-bound session to succeed through west, restores east, waits for Kubernetes readiness, and requires the original mapping to become usable again under the same HTTPS name. Cleanup restores the Deployment even if verification exits early. |

## Grid Discovery And Overlay Rendering

### User Story

As a platform operator, I need provider availability and capacity changes to
reach every edge without placing a remote control-plane call on the inference
request path.

### Summary

The Grid operators in the four edge/provider clusters exchange site and
provider facts over SWIM. Each edge operator independently renders the routing
view for its local Praxis edge.

### Technical Implementation

Each edge `GridNetwork` declares one `gatewayRef` named `edge-gateway` and a
distinct `localSiteName`. The operator creates:

```text
ConfigMap/grid-overlay-glb-demo-edge-gateway
  data:
    routing-overlay.json: <versioned routing envelope>
    routing-config.json: <legacy bare routing payload>
```

The local `edge-gateway` Deployment projects that ConfigMap at:

```text
/etc/praxis/routing/routing-overlay.json
```

Praxis AI's `intelligent_route` filter watches that file and reloads eligible
candidates after the configured debounce interval. Kubernetes' native
ConfigMap projection is the distribution mechanism inside the cluster. No
overlay sidecar, host process, or second operator is used.

SWIM and overlay rendering have distinct responsibilities:

```text
provider facts
  -> SWIM replication among Grid operators
  -> edge-local Grid reconciliation
  -> edge-local routing overlay ConfigMap
  -> Kubernetes projected volume
  -> Praxis intelligent_route hot reload
```

The overlay contains stable candidate identity, site, cluster, admission state,
selection tier, rank, and provider credential reference metadata. The edge
config maps the candidate clusters to the provider gateway LoadBalancer
addresses and requires verified upstream mTLS.

### What The Demo Proves

The verifier reads both operator-owned ConfigMaps and requires each one to:

- identify the edge that owns the view through `local_site`;
- contain all three provider candidates before failure testing, including two
  distinct candidates for `sim-model-v1` at the east site;
- carry the expected provider address, model, admission, rank, and credential
  reference metadata; and
- be mounted into the Praxis pod that serves that edge.

This proves more than eventual provider discovery. It proves that replicated
provider facts became a usable, edge-specific data-plane input without adding
a control-plane lookup to each inference request.

## Demo 1: Active/Active Edge And Provider Routing

### User Story

As an application owner, I need one stable HTTPS inference endpoint backed by
active edges while Grid independently selects an admitted provider.

### Summary

One verified HTTPS name reaches two active Praxis edges. After the GTM emulator
selects a healthy edge, that edge independently selects an eligible inference
provider from its local, operator-rendered Grid overlay. Both edge overlays
contain three provider candidates: two at the east site and one at the west
site. This proves provider identity remains distinct from cluster and gateway
identity, and the edge location does not constrain the provider location.
Separate affinity keys keep those decisions observable and independent.

### Scenario Flow

```mermaid
flowchart LR
    Client[Inference client] -->|One HTTPS name| GTM[Praxis GTM emulator]
    GTM --> EastEdge[East Praxis edge]
    GTM --> WestEdge[West Praxis edge]
    EastEdge -->|Primary or secondary east candidate| EastGateway[East provider gateway]
    EastEdge -->|Crossed route| WestGateway[West provider gateway]
    WestEdge -->|Crossed route| EastGateway
    WestEdge -->|Grid provider selection| WestGateway
    EastGateway --> EastBackend[Primary east private backend]
    EastGateway --> EastBackend2[Secondary east private backend]
    WestGateway --> WestBackend[West private backend]
```

### Technical Implementation

The emulator terminates TLS for `api.grid-glb.test:8443` and load-balances to:

```text
east-edge  Service/edge-gateway:8080
west-edge  Service/edge-gateway:8080
```

The emulator-to-edge segment is plaintext inside the isolated local cluster
network. A production GTM deployment uses authenticated origin transport,
normally TLS or mTLS, between the global ingress tier and each public edge.

`X-Edge-Session-Id` supplies deterministic demo stickiness. The verifier finds
session values that reach each edge, repeats them to prove stability, scales
the east edge Deployment to zero, waits for withdrawal to west, restores east,
and proves recovery under the same hostname.

The emulator's consistent-hash topology is local to one process. Production
stickiness semantics belong to the selected managed GTM product.

### Independent Grid Provider Selection

The edge pipeline is:

```text
OpenAI-compatible request parsing
  -> edge attribution
  -> intelligent_route
  -> mTLS load_balancer
  -> selected provider gateway
```

`intelligent_route` selects from the local Grid overlay, applies admission state and
rank, preserves provider session affinity, and emits authenticated
`x-ai-routing-*` hop context for the selected provider. The following
`load_balancer` owns only transport to the selected cluster.

The two east candidates advertise the same `sim-model-v1` model and the same
`east-provider` site, but use different `cluster` and `stable_id` values. The
east provider gateway validates each candidate independently, selects its
matching private backend, and injects that provider's credential. The verifier
requires both candidates in both edge overlays, sends a request to the
highest-ranked east provider, temporarily stops that provider from accepting
new sessions, and proves the next new session reaches the other east provider.
It then restores the original admission state.

The [end-to-end request path](#end-to-end-request-path) shows the full
crossed edge/provider path and the policy enforced at each hop.

The topology supports these paths:

```text
east edge -> east provider
east edge -> second east provider
east edge -> west provider
west edge -> east provider
west edge -> second east provider
west edge -> west provider
```

Normal selection remains score- and rank-driven, so the verifier does not
force all six combinations while provider state is unchanged. It proves both
active edges, explicitly routes through both providers hosted at the east
site, proves provider withdrawal, and reports any crossed edge/provider path
observed in live responses.

### What The Demo Proves

- The stable HTTPS name returns a successful attributed inference response.
- Bounded affinity-key discovery finds live requests served by both edges,
  proving that neither edge is merely a configured standby.
- Both edge overlays contain all three provider candidates, including the two
  independent providers hosted at the east site.
- Response attribution distinguishes the outer edge choice from the inner
  provider choice and reports crossed paths when they occur.
- Withdrawing one edge or provider preserves service through the corresponding
  healthy alternative.

## Demo 2: Secure Provider Boundary

### User Story

As a provider owner, I need Grid traffic authenticated and authorized before
it can reach my inference backend.

### Summary

The provider Service selects a Praxis provider gateway, never the backend
directly. That gateway establishes a second security boundary: it authenticates
the calling edge, authorizes the exact candidate, model, and path, removes the
consumer credential, and injects a provider-local credential only for the
final backend hop. The backend remains a private `ClusterIP` Service protected
by NetworkPolicy.

The provider gateway uses a MetalLB address so the other local clusters can
reach it. In production this endpoint belongs on private inter-site
connectivity or an internal load balancer; mTLS and peer authorization remain
mandatory even on that private network.

### Scenario Flow

```mermaid
sequenceDiagram
    participant Client as Inference client
    participant Edge as Praxis edge
    participant Provider as Praxis provider gateway
    participant Backend as Private backend

    Client->>Edge: Request with consumer credential
    Edge->>Provider: mTLS plus authenticated Grid route context
    Provider->>Provider: Authenticate edge identity
    Provider->>Provider: Validate candidate, model, and path
    Provider->>Provider: Replace consumer credential
    Provider->>Backend: Request with provider-local credential
    Backend-->>Provider: Authorized response
    Provider-->>Edge: Provider attribution
    Edge-->>Client: Inference response
```

### Technical Implementation

The provider pipeline is:

```text
required downstream mTLS
  -> peer_identity_trust
  -> model extraction
  -> provider_route
  -> credential_inject
  -> local backend load_balancer
```

Each provider:

- requires a client certificate signed by the demo CA;
- pins both edge certificate digests and the `ai-grid` organization;
- strips and validates the authenticated `x-ai-routing-*` context;
- maps exact candidate, model, and path tuples to one local backend;
- rejects unknown candidates, models, and paths;
- removes the external client's `Authorization` value;
- injects a provider-local credential only on the backend hop; and
- forwards to the private `mock-inference` Service.

The verifier proves:

| Probe | Expected result |
|---|---|
| Valid east or west edge identity | TLS accepted |
| No client certificate | TLS refused |
| Wrong CA or SNI | TLS refused |
| Valid CA, wrong organization | HTTP 403 |
| Valid organization, untrusted digest | HTTP 403 |
| Unknown candidate | HTTP 403 |
| Unsupported path or model | Rejected |

The successful probe must also return matching edge, provider-gateway, backend,
and backend-request attribution. A TLS handshake alone is not counted as a
successful provider route, and a resource manifest alone is not counted as
proof of enforcement.

### Credential And Backend Isolation

#### User Story

As a provider owner, I need my backend credential and private service to remain
inside my provider site, even when requests arrive through a remote public
edge.

#### Summary

The provider gateway and backend share a Kubernetes Secret local to their
provider cluster. The gateway reads the value from a mounted file; the backend
uses the same Secret only to verify the demo request.

#### Technical Implementation

The verifier sends `Authorization: Bearer test-token` to the public edge. The
strict backend proves that this client-supplied fixture is not accepted:

| Direct private-backend probe | Result |
|---|---|
| No authorization | HTTP 401 |
| Client-supplied fixture token | HTTP 403 |
| Provider gateway path | HTTP 200 |

The successful response includes bounded demo attribution from the edge,
provider gateway, and backend request capture. Secret values and private keys
are never printed as evidence.

The NetworkPolicy verifier uses the same probe image and target for both
cases. A provider-gateway-labeled pod must connect; an otherwise identical
unlabeled pod must fail. This is runtime enforcement evidence, not manifest
inspection.

## Demo 3: Session Affinity, Drain, And Hot Reload

### User Story

As an inference client, I need related requests to remain on one eligible
provider while allowing operators to drain new work without restarting the
edge.

### Summary

Related requests remain on the same eligible edge and provider, while
operators can stop a busy provider from receiving new sessions without
breaking established work. A queue-depth change moves provider admission to
`existing_only`; Grid propagates and renders that state, and both running
Praxis edges reload it without a pod replacement or request-path control-plane
lookup.

There are two independent affinity layers:

| Layer | Header | Decision |
|---|---|---|
| GTM emulator | `X-Edge-Session-Id` | East or west edge |
| Grid edge | `X-Session-Id` | East or west provider |

### Scenario Flow

```mermaid
flowchart TD
    Metric[Provider queue depth rises] --> Admission[Provider admission becomes existing_only]
    Admission --> SWIM[SWIM distributes provider state]
    SWIM --> Overlays[Edge operators render updated local overlays]
    Overlays --> Projection[Kubernetes projects ConfigMaps into edge pods]
    Projection --> Reload[intelligent_route validates and hot reloads]
    Reload --> Existing[Bound session stays on the draining provider]
    Reload --> New[New session selects the alternate provider]
    Recovery[Queue depth recovers] --> Admission
```

### Technical Implementation

The two keys intentionally solve different problems. The GTM emulator uses
consistent hashing on `X-Edge-Session-Id` to choose a healthy edge.
`intelligent_route` uses `X-Session-Id` to bind a session to an eligible provider for
up to the configured one-hour TTL. A crossed route can therefore keep the
client on the east edge while keeping its provider session on the west
provider.

The provider-affinity proof first sends a request with a new
`X-Session-Id`, records the selected provider from response attribution, and
repeats the request twice. All three responses must identify the same provider.

The drain proof then changes the selected mock backend's normalized queue-depth
metric from the ready value `0.10` to `0.95`. The operator's admission policy
marks a provider `existing_only` above its `0.85` queue threshold. The change
travels through the normal control-plane path:

```text
mock backend exports queue metric
  -> provider operator scrapes and normalizes the metric
  -> InferenceProvider admission becomes existing_only
  -> SWIM distributes the provider fact
  -> each edge operator reconciles its local overlay ConfigMap
  -> Kubernetes updates the projected file in the edge pod
  -> intelligent_route validates and reloads the new overlay
```

The edge configuration watches the versioned
`/etc/praxis/routing/routing-overlay.json` envelope with hot reload enabled and a 500 ms
debounce. The same ConfigMap also carries the legacy `routing-config.json`
payload during the compatibility transition. Requests continue using the last
accepted in-memory view while the projected file settles; the request path
does not parse the ConfigMap or call the operator.

Each semantic routing change receives a content-addressed revision. The full
demo follows that exact value through four independently observed stages:

```text
Grid renders revision A
  -> Kubernetes distributes revision A in the ConfigMap
  -> Praxis validates and accepts revision A
  -> a routed request proves it was served by revision A
```

Formatting, timestamps, and provenance do not change the semantic revision.
Candidate order, admission, endpoints, credential references, and other
routing content do. The verifier rejects any disagreement between the
rendered, distributed, accepted, and serving values.

### What The Demo Proves

The verifier requires all of the following:

- the existing session remains on the selected provider;
- a newly generated session avoids the `existing_only` provider;
- the operator-owned overlay contains the new admission state;
- the running edge emits a later `overlay reloaded` event; and
- the edge pod UID and restart count remain unchanged.

The verifier then scales the selected provider backend to zero. Provider
health reconciliation marks the `InferenceProvider` unavailable, SWIM carries
that state to the edge operators, and the selected provider disappears from
the generated edge overlay. The running Praxis edge reloads the projected
overlay and routes a new request through the remaining provider.

Cleanup restores the backend replica count and queue metric, requires the
provider to return to `Available`, requires both edge candidates to return to
`new_and_existing`, and confirms another live reload. The verifier does not
patch operator-owned overlay or status resources to simulate provider drain,
withdrawal, or recovery.

Full mode finishes with a bounded failure-safety proof.  The verifier
temporarily pauses the edge Grid operator (controlled fault injection)
so its self-healing reconciliation does not overwrite the deliberately
corrupted `ConfigMap` — this is not a production overlay-distribution
path.  With the operator paused, the verifier writes an invalid content
digest to the envelope key specifically to exercise the contract
validator, requires the running edge to reject the update while
continuing to serve its last-known-good revision, and then replaces the
edge pod while the invalid content is still mounted.  It never patches
Grid status.  The replacement must remain unready, proving that cold
startup fails closed.  The verifier restores the valid envelope and the
original operator replica count, then requires the edge and inference
path to recover before teardown.  Both restoration steps are attempted
independently so that a `ConfigMap` failure does not leave the operator
scaled down.

## Demo 4: Failure, Withdrawal, And Recovery

### User Story

As a reliability operator, I need edge and provider failures to withdraw only
the affected path, preserve service through healthy alternatives, and recover
without manual routing edits.

### Summary

The demo creates real edge and provider failures by changing live Kubernetes
workloads, not by patching Grid-owned status or overlay data. Edge health
withdrawal preserves the stable HTTPS endpoint through the remaining edge.
Provider health withdrawal removes an unavailable candidate from both edge
views, after which the running gateways reload and continue through the
remaining provider. Restoration must return the failed path through normal
health, SWIM, reconciliation, and readiness behavior.

### Scenario Flow

```mermaid
flowchart TD
    subgraph EdgeFailure[Edge withdrawal and recovery]
        EastDown[East edge scales to zero] --> GTMWithdraw[GTM health checks withdraw east]
        GTMWithdraw --> WestServes[Same HTTPS name routes through west edge]
        WestServes --> EastRestore[East edge is restored]
        EastRestore --> GTMReturn[Health checks return east to service]
    end

    subgraph ProviderFailure[Provider withdrawal and recovery]
        BackendDown[Selected backend scales to zero] --> ProviderUnavailable[Provider becomes unavailable]
        ProviderUnavailable --> GridUpdate[SWIM and edge reconciliation remove candidate]
        GridUpdate --> PraxisReload[Praxis edges hot reload]
        PraxisReload --> Alternate[Requests use the remaining provider]
        Alternate --> ProviderRestore[Backend and provider return]
    end
```

### Edge Withdrawal

**User Story:** As an inference consumer, I need the same public hostname to
continue working when an edge site becomes unavailable.

**Summary:** The GTM layer removes an unhealthy edge from new selections and
continues through the other active edge. Grid provider selection still happens
after that edge decision.

**Implementation and proof:**

```text
before: client -> GTM emulator -> east edge -> provider

failure:
  east edge Deployment scaled to zero
  -> emulator health check withdraws east
  -> same hostname reaches west edge

recovery:
  east edge restored
  -> readiness and health recover
  -> same hostname and edge session reach east again
```

The verifier scales the east edge Deployment to zero rather than editing the
GTM endpoint list. The emulator's one-second TCP health check requires two
failed checks before withdrawal. The east-bound affinity key must then receive
HTTP 200 from the west edge under the same HTTPS name. After restoration, the
Deployment must become ready and the original key must be served by east
again.

### Provider Drain

**User Story:** As a provider operator, I need to stop admitting new sessions
before maintenance while allowing already bound work to continue.

**Summary:** Capacity pressure changes provider admission, not basic health.
The provider remains reachable for existing sessions while new sessions move
to the other admitted provider.

**Implementation and proof:**

```text
mock queue metric rises above 0.85
  -> provider operator derives existing_only
  -> SWIM distributes provider state
  -> edge operator renders the admission change
  -> Praxis reloads without restarting

existing session -> selected provider remains eligible for existing work
new session      -> alternate admitted provider
```

The proof starts with a known provider binding, raises the selected provider's
queue metric to `0.95`, waits for the edge overlay to show
`existing_only`, and waits for a live reload. It then requires the old session
to retain its provider and a newly generated session to select the alternate
provider.

### Provider Unavailability

**User Story:** As a platform operator, I need a provider that can no longer
serve requests removed from every edge's eligible routing view.

**Summary:** Health failure removes the provider candidate, rather than asking
each edge or consumer to discover the failure independently.

**Implementation and proof:**

```text
selected backend becomes unavailable
  -> InferenceProvider phase becomes Unavailable
  -> SWIM distributes the health change
  -> edge operator removes the candidate
  -> Praxis reloads and routes through the remaining provider

recovery restores the backend, provider phase, candidate, and live edge view
```

The verifier scales the selected backend to zero and waits for the provider
health controller, SWIM propagation, edge reconciliation, and Praxis reload.
It requires a successful request through the remaining provider and confirms
that the edge pod was neither replaced nor restarted. Cleanup restores the
backend and requires the candidate and another reload to return.

### Invalid Provider Peer

**User Story:** As a provider owner, I need possession of network connectivity
or a certificate from the wrong identity domain to be insufficient for backend
access.

**Summary:** Provider access is authenticated and authorized at the provider
gateway before route selection or credential injection.

**Implementation and proof:**

```text
untrusted client
  -> provider TLS/peer boundary
  -> rejected
  -> private backend never receives the request
```

Negative probes cover no certificate, the wrong CA, the wrong SNI, the wrong
organization, and an untrusted certificate digest. Valid edge identities must
complete TLS and pass peer policy; the negative identities must fail at the
expected layer.

## Demo 5: Grid Restart Recovery And Request Soak

### User Story

As a platform operator, I need control-plane pod replacement to preserve
converged routing, and I need successful inference traffic to remain stable
after that recovery.

### Summary

Full mode restarts each of the four Grid operators one at a time. After every
restart, the replacement Deployment must become ready, both edge overlays must
still contain all three providers, and a request must complete through the stable
HTTPS endpoint. The demo then sends requests for the configured soak interval
and duration, requiring every request to succeed while reaching both edges.

```mermaid
flowchart LR
    Restart[Restart one Grid operator] --> Ready[Replacement pod becomes ready]
    Ready --> Overlay[Both edge overlays remain complete]
    Overlay --> Request[Inference request succeeds]
    Request --> Next{More Grid operators?}
    Next -->|yes| Restart
    Next -->|no| Soak[Configured request soak]
    Soak --> Proof[All requests pass through both edges]
```

Operators are restarted sequentially so the test exercises normal rolling
maintenance rather than intentionally taking the entire discovery mesh down.
Quick mode skips this longer durability proof.

## Live OpenAI Provider (Optional)

The demo can optionally route one real inference request through a live OpenAI
endpoint. When enabled, the east provider gateway gains a fourth upstream
(`openai-api`) alongside the three simulated providers. No sixth cluster is
created; the OpenAI route shares the east provider gateway's mTLS boundary.

### What It Proves

The request follows the full Grid data path:

```text
client -> GTM emulator -> selected edge -> east provider gateway -> api.openai.com:443
```

This proves that Grid provider selection, the authenticated provider hop,
credential injection, and upstream TLS work with a real external API, not only
simulated backends. The caller sends `Authorization: Bearer invalid-caller-token`
to prove credential replacement at the provider gateway.

### Prerequisites

- An OpenAI API key stored in a file outside the repository.
- The file must be owned by the current user, mode `0600` (not group- or
  world-readable), regular (not a symlink), and no larger than 4096 bytes.
- Outbound HTTPS to `api.openai.com:443` from the east-provider Kind cluster.

### Usage

```bash
cargo xtask env run-grid-glb-demo \
  --quick \
  --teardown \
  --external-provider openai \
  --external-provider-key-file /path/to/openai-key \
  --external-provider-model gpt-4o-mini
```

The three `--external-provider*` flags are all-or-nothing. Omitting all three
runs the standard simulated-only demo. Providing `--external-provider` without
`--external-provider-key-file` or `--external-provider-model` is rejected at
preflight before any clusters are created.

### Billing

The demo sends one non-streaming request with `max_output_tokens: 16`. Typical
cost is a fraction of a cent. No periodic health probes or retries are sent.

### Request Format

The request uses the OpenAI Responses API (`/v1/responses`) in native format.
No translation layer or prompt rewriting is applied.

### Security And Evidence

- The API key is never read by the Rust process. It is passed to
  `kubectl create secret --from-file` which reads it directly.
- The key file path, content, length, prefix, suffix, hash, and fingerprint
  are never logged, narrated, or included in evidence.
- The key is stored as a Kubernetes Secret mounted into the provider gateway
  pod. It does not appear in ConfigMaps, YAML templates, command arguments,
  environment variables, or the evidence report.
- Evidence records only: HTTP status, provider identity, model, and timing.
  Prompt text, response body, and OpenAI request identifiers are not retained.

### Common Errors

| Status | Likely cause |
|---|---|
| 401 | Invalid or expired API key |
| 403 | Key lacks permission for the requested model |
| 404 | Model name not recognized by OpenAI |
| 429 | Rate limit or quota exceeded |
| DNS/TLS failure | No outbound connectivity from the Kind cluster |

### Not In Scope

- Anthropic, AWS Bedrock, Google Vertex, or other provider APIs.
- Request format translation between provider APIs.
- Streaming responses.
- Periodic health probing of the external endpoint.
- Automatic Secret management by the Grid operator (`auth.manual: true`).

## Run The Demo

### Quickstart

Clone Grid, select published immutable image references, and run the quick
setup plus narration:

```bash
if ! command -v cargo >/dev/null 2>&1; then
  curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs |
    sh -s -- -y
  . "$HOME/.cargo/env"
fi

git clone https://github.com/praxis-proxy/grid.git
cd grid

export GRID_XTASK_GATEWAY_IMAGE=ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.1
export GRID_XTASK_OPERATOR_IMAGE=ghcr.io/praxis-proxy/grid-operator:v0.1.1
export GRID_XTASK_MOCK_PROVIDER_IMAGE=ghcr.io/praxis-proxy/grid-mock-providers:v0.1.1
export GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent

printf 'Gateway: %s\nOperator: %s\nMock provider: %s\nPull policy: %s\n' \
  "$GRID_XTASK_GATEWAY_IMAGE" \
  "$GRID_XTASK_OPERATOR_IMAGE" \
  "$GRID_XTASK_MOCK_PROVIDER_IMAGE" \
  "$GRID_XTASK_IMAGE_PULL_POLICY"

cargo build -p forge

cargo xtask env run-grid-glb-demo \
  --forge-config demos/grid-glb-demo/forge.yaml \
  --quick \
  --teardown \
  2>&1 | tee grid-glb-demo-output.txt
```

These three digest references are a mutually compatible validation set. The
Praxis AI image rolls up the open intelligent-routing PR stack while those
changes are reviewed upstream. The explicit build makes
`target/debug/praxis-forge` available to the xtask runner. The demo command
prints the resolved image contract again before loading images, then creates
five single-node Kind clusters, pulls the declared images, deploys the
environment, and runs the core routing and security displays. If the printed
references differ from the block above, stop and correct stale shell
overrides before investigating a workload readiness timeout. A non-zero exit
means at least one runtime assertion failed; the complete narration remains in
`grid-glb-demo-output.txt` and machine-readable results are written to the
evidence directory (see
[Generated Artifacts](#generated-artifacts)).

Rerun only the narration without recreating the environment:

```bash
cargo xtask env demonstrate-grid-glb \
  --forge-config demos/grid-glb-demo/.forge.resolved.yaml \
  --quick \
  2>&1 | tee grid-glb-demo-rerun.txt
```

Remove the five clusters with `--teardown` (preferred) or manually:

```bash
cargo run -p forge -- \
  --config demos/grid-glb-demo/.forge.resolved.yaml \
  --non-interactive down --force
```

### Prerequisites

- Linux with Docker;
- Kind, kubectl, curl, and OpenSSL on `PATH`;
- Rust and the repository-pinned nightly toolchain;
- a locally built `praxis-forge` binary (`cargo build -p forge`);
- capacity for five single-node Kind clusters; and
- either the three local demo images or accessible registry images.

Validate the declarative environment:

```bash
cargo run -p forge -- \
  config validate \
  --config demos/grid-glb-demo/forge.yaml
```

### Registry Images

Use the published release images:

```bash
export GRID_XTASK_GATEWAY_IMAGE=ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.1
export GRID_XTASK_OPERATOR_IMAGE=ghcr.io/praxis-proxy/grid-operator:v0.1.1
export GRID_XTASK_MOCK_PROVIDER_IMAGE=ghcr.io/praxis-proxy/grid-mock-providers:v0.1.1
export GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent
```

Use `IfNotPresent` only when every image is available from a registry. If any
image reference is local, set the policy to `Never`, pull any registry images
into the local container engine first, and let the xtask load all three images
into the Kind clusters.

`GRID_XTASK_GATEWAY_IMAGE` must include `intelligent_route`, versioned overlay
validation, accepted/serving revision evidence, `provider_route`,
`credential_inject`, hot reload, downstream mTLS, upstream mTLS, and peer
identity trust. The legacy mock-EPP image is not used by this demo.

### Local Images

With no overrides, setup expects:

```text
praxis-ai:glb-demo
grid-operator:glb-demo
grid-mock-providers:glb-demo
```

Build the Grid-owned images with:

```bash
make glb-demo-images
```

Build `praxis-ai:glb-demo` from a compatible AI integration branch. The Grid
repository does not vendor Praxis AI.

### Setup And Narration

```bash
cargo build -p forge
cargo xtask env run-grid-glb-demo \
  --quick \
  --teardown \
  2>&1 | tee grid-glb-demo-output.txt
```

See [e2e-demo-output.txt](e2e-demo-output.txt) for example narrated output
from a quick cold run.

The command:

1. resolves image overrides without changing source manifests;
2. generates distinct edge, provider, untrusted-peer, and public-name identities;
3. creates all five Kind clusters on one cross-cluster network;
4. loads local images only when pull policy is `Never`;
5. installs MetalLB in all five clusters;
6. installs Grid and the four-member SWIM mesh in edge/provider clusters;
7. installs Kubernetes TLS and credential Secrets;
8. deploys three private provider paths behind two provider gateways and
   captures both gateway addresses;
9. deploys both edge sites and mounts each operator-rendered overlay;
10. deploys the GTM emulator after both edge addresses are known;
11. runs the Grid routing and provider-boundary proof;
12. performs a basic session-affinity check.

Full mode continues by proving repeated two-layer affinity and provider drain,
running Kubernetes edge withdrawal, recovery, and failback, restarting all four
Grid operators sequentially, and running a configured request soak through the
stable HTTPS endpoint.

Setup and narration can be run separately:

```bash
cargo xtask env setup-grid-glb

cargo xtask env demonstrate-grid-glb \
  --forge-config demos/grid-glb-demo/.forge.resolved.yaml \
  --quick \
  2>&1 | tee grid-glb-demo-output.txt
```

### Demo Modes

The demo supports two modes that control scenario depth:

| Mode | Flag | Scenarios |
|---|---|---|
| Quick (recommended first run) | `--quick` | Active/active routing, three provider candidates including two in one cluster, the rendered-to-serving overlay revision chain, inference requests, a basic affinity check, and the secure provider boundary |
| Full (extended validation) | `--full` | Every quick check plus repeated edge/provider affinity, provider drain, edge withdrawal/recovery, sequential Grid operator restart recovery, and a configured request soak |

Quick mode runs scenarios 1-2. It creates the same five-cluster topology as
full mode, so it still proves the real deployment, discovery, request, and
security path. A bounded admission change proves both providers in the east
cluster can serve new sessions, then restores the initial state. Quick mode
skips the longer session-retention and lifecycle exercises.

Full mode runs all five scenarios. Provider drain, edge withdrawal and
recovery, four sequential operator restarts, and the configured request soak
make it the extended validation path. Use it for release validation and changes
that affect routing state, recovery, or the distributed runtime. The flags are
mutually exclusive; omitting both currently selects full mode.

```bash
# Quick mode: normal first run
cargo xtask env run-grid-glb-demo --quick --teardown

# Full mode: extended lifecycle and resilience validation
cargo xtask env run-grid-glb-demo --full --teardown
```

### Lifecycle Controls

| Flag | Behavior |
|---|---|
| `--teardown` | Delete all Kind clusters after setup and proof execution, including after a failure |
| `--keep-on-failure` | With `--teardown`, retain a partially or fully deployed environment when a proof fails |
| `--evidence-dir <path>` | Override the default evidence output directory |

```bash
# Run and clean up
cargo xtask env run-grid-glb-demo --quick --teardown

# Run, but keep clusters if something fails
cargo xtask env run-grid-glb-demo --quick --teardown --keep-on-failure

# Specify a custom evidence directory
cargo xtask env run-grid-glb-demo \
  --quick \
  --evidence-dir .forge/evidence/manual-run
```

### Generated Artifacts

Each run writes machine-readable evidence to a timestamped directory under
`.forge/evidence/` (or the path given by `--evidence-dir`):

```text
.forge/evidence/glb-demo-20260728T120000Z/
  narration.txt     # High-level scenario narration and final summary
  results.json      # Structured evidence report
```

The `results.json` file uses a versioned schema (`schema_version: "1"`) and
contains:

- **mode**: `"quick"` or `"full"`
- **status**: `"pass"` or `"fail"`
- **error**: bounded, single-line failure detail when the run fails
- **capabilities**: per-scenario result, evidence string, and pass/fail/skipped
- **observed_paths**: response-derived edge/provider routing paths
- **lifecycle**: teardown actions performed and their results
- **artifacts**: paths to generated files

The evidence schema does not collect Secret values, private keys, bearer
tokens, or credentials. Treat the separately captured combined command output
as operational logs and review it before sharing. The `.forge/` directory is
in `.gitignore`.

## Grid Routing And Provider-Boundary Proof

The focused Grid proof reports assertions by capability rather than exposing a
fixed step count:

| Capability group | Runtime assertions |
|---|---|
| Environment identity | Forge configuration is valid and all five declared clusters are live |
| Grid mesh | Four SWIM LoadBalancer Services exist, advertised addresses match, and every GridNetwork has the other three seeds |
| Edge overlays | Both edge-local ConfigMaps exist, identify the correct local edge, contain all three provider candidates with distinct identities for the two east providers, and are projected into the matching Praxis pod |
| Provider discovery | Both provider gateway addresses match their Services and the remote GridSite egress values propagated through SWIM |
| Provider workload boundary | Provider Services select Praxis pods on `8443`; backends are `ClusterIP`; labeled and unlabeled probes prove NetworkPolicy enforcement |
| TLS and peer trust | Both edge identities succeed; missing certificate, wrong CA, wrong SNI, wrong organization, and untrusted digest fail |
| Provider-local policy | Unknown candidate, unsupported model, and unsupported path are rejected after peer authentication |
| Credential boundary | The backend rejects the consumer-supplied `Authorization` credential; the provider gateway replaces it with a provider-local credential for the successful final hop |
| End-to-end routing | A direct edge request returns HTTP 200 with matching edge, provider-gateway, backend-provider, and backend-request attribution |
| Provider session behavior | Initial binding and repeated reuse are stable; existing work survives `existing_only` while new work selects the alternate provider |
| Hot reload | Provider health withdrawal removes one candidate through normal reconciliation and SWIM propagation; the running edge reloads, routes through a remaining provider, then reloads the restored three-provider view without changing pod UID or restart count |

This proof covers Grid's provider-routing path. The separate GTM emulator proof
covers the stable public name, both edge identities, edge affinity, edge
withdrawal, recovery, and failback.

Run the focused verifiers:

```bash
cargo xtask env verify-grid-glb-routing \
  --forge-config demos/grid-glb-demo/.forge.resolved.yaml

cargo xtask env verify-grid-glb-gtm-emulator \
  --forge-config demos/grid-glb-demo/.forge.resolved.yaml
```

## Evidence Output

The narrated output includes:

- a runtime PASS/FAIL table;
- all five Praxis workload identities;
- both edge overlay perspectives;
- the observed active-edge/provider paths;
- provider mTLS and peer-policy negative probes;
- backend NetworkPolicy and credential-replacement evidence;
- provider affinity, drain, hot-reload, and pod-stability evidence; and
- edge withdrawal, recovery, and stable-URL evidence.

### Request, Provider-Hop, And Demo Evidence Headers

The narrated proof uses three different header categories. Only the
`X-Grid-Demo-*` response fields are client-visible test evidence. Session
inputs drive configured affinity behavior, while `x-ai-routing-*` fields are
the authenticated edge-to-provider protocol and must not be treated as
demo-only attribution.

#### Affinity And Request Metadata

| Header | Set by | Scope and purpose |
|---|---|---|
| `X-Edge-Session-Id` | Verifier client | Affinity input configured on the GTM emulator. Consistent hashing maps the value to a healthy edge so repeated requests can prove edge stability. A production GTM owns its own affinity contract. |
| `X-Session-Id` | Verifier client | Affinity input configured on `intelligent_route`. It binds related requests to an eligible provider and allows the verifier to distinguish established sessions from new sessions during drain. The configured header name is deployment-specific. |
| `X-Model` | `json_body_field` | Gateway-local routing signal extracted from the request body's `model` field. It lets `intelligent_route` and `provider_route` match the requested model without reparsing the body. It is internal request metadata, not path-attribution evidence. |

#### Authenticated Provider-Hop Protocol

| Header | Set by | Scope and purpose |
|---|---|---|
| `x-ai-routing-candidate` | Edge `intelligent_route` | Carries the stable candidate selected from the edge overlay. The provider validates it against provider-owned candidate, model, and path policy. |
| `x-ai-routing-request-id` | Edge `intelligent_route` | Bounded correlation identifier for the authenticated provider hop. |
| `x-ai-routing-revision` | Edge `intelligent_route` | Carries the content-addressed revision from the exact overlay snapshot used to select the provider. The provider validates its bounded SHA-256 form and treats it as correlation evidence, not authorization. |

The edge removes client-supplied copies before setting these fields. They are
sent only for clusters explicitly configured as provider hops. The provider
consumes them only after downstream mTLS and `peer_identity_trust`, then removes
them before the backend request. No credential reference or credential value
crosses this boundary.

For backend-side proof, `provider_route` replaces any inbound
`x-ai-provider-attribution`, `x-ai-provider-request-id`, and
`x-ai-provider-routing-revision` values with provider-owned identity,
correlation, and validated revision values. The strict mock backend reflects
those bounded values under demo response names, proving that the request passed
through the provider pipeline rather than reaching the backend directly. The
edge-owned `x-ai-routing-*` fields never reach the backend.

#### Demo-Only Response Evidence

| Header | Set by | Verifier assertion |
|---|---|---|
| `X-Grid-Demo-Edge-Gateway` | Edge `headers` configuration | Identifies the edge that handled the request and proves both edges serve traffic. |
| `X-AI-Demo-Provider-Gateway` | `provider_route` when `emit_demo_attribution: true` | Identifies the provider gateway that accepted and authorized the provider hop. |
| `X-Grid-Demo-Provider` | Strict mock backend | Identifies the backend provider site that produced the response. |
| `X-Grid-Demo-Backend-Provider-Attribution` | Strict mock backend | Reflects the provider-owned attribution received by the backend and must match the provider gateway. |
| `X-Grid-Demo-Backend-Request-Id` | Strict mock backend | Reflects the provider-owned correlation ID and must be present and non-empty. |
| `X-Grid-Demo-Backend-Overlay-Revision` | Strict mock backend | Reflects the provider-owned revision and must match the rendered, distributed, accepted, and serving revision for the request. |

These response fields exist solely so the verifier can make runtime assertions
about the observed path. The edge field uses ordinary demo configuration. The
provider-gateway field is an opt-in mode of `provider_route`; it is not
emitted unless `emit_demo_attribution` is enabled. The remaining fields come
from `mock-providers`, which is test infrastructure rather than a production
backend.

No authorization decision trusts a demo response field. A production
deployment would normally disable demo attribution and use distributed
tracing, metrics, or access logs for path evidence. The authenticated
`x-ai-routing-*` provider-hop contract remains part of the real Grid data path.

## Repository Layout

| Area | Location |
|---|---|
| Environment orchestration | `forge.yaml` |
| Edge Praxis pipeline | `configs/edge/praxis.yaml` |
| Provider Praxis pipelines | `configs/east-provider/`, `configs/west-provider/` |
| GTM emulator Praxis pipeline | `configs/gtm-emulator/praxis.yaml` |
| GridNetwork and GridSite resources | `resources/gridnetwork-*.yaml`, `resources/site-*.yaml` |
| Inference provider declarations | `resources/inference-*.yaml` |
| Kubernetes edge workload | `resources/edge-gateway-*.yaml` |
| Kubernetes provider boundary | `resources/provider-*.yaml`, `resources/backend-network-policy.yaml` |
| Kubernetes GTM emulator | `resources/gtm-emulator-*.yaml` |
| Setup and narration | `xtask/src/env/glb_demo.rs` |
| Provider and hot-reload verifier | `xtask/src/env/glb.rs` |
| GTM emulator verifier | `xtask/src/env/gtm_emulator.rs` |

## Outstanding Integration Pull Requests

The integration image used by this demo combines these AI changes:

| Pull request | Scope | Relationship |
|---|---|---|
| [praxis-proxy/ai#339](https://github.com/praxis-proxy/ai/pull/339) | `intelligent_route`, provider-hop context, and selected-candidate routing | Base Grid routing capability |
| [praxis-proxy/ai#540](https://github.com/praxis-proxy/ai/pull/540) | Overlay-file hot reload | Builds on `intelligent_route` |
| [praxis-proxy/ai#386](https://github.com/praxis-proxy/ai/pull/386) | Provider-local route validation and credential injection | Consumes authenticated provider-hop context |

The three PRs remain independently owned even when a temporary integration
image combines them for end-to-end validation.

## Capability Guide

### Proven In This Demo

- **One Praxis runtime in three roles:** live Kubernetes Deployments run the
  GTM emulator, both public edges, and both private provider gateways from the
  same Praxis AI image with role-specific pipelines.
- **Local request-time routing:** four Grid operators exchange provider facts
  over SWIM, each edge receives its own rendered overlay, and Praxis routes
  without a request-time control-plane call.
- **Two independent routing layers:** one verified HTTPS name reaches both
  active edges, while Grid independently selects from three providers and
  reports crossed edge/provider paths from response attribution.
- **Provider security boundary:** mTLS, certificate identity, peer policy,
  candidate/model/path validation, final-hop credential replacement, and
  NetworkPolicy-enforced backend isolation all receive positive and negative
  runtime probes.
- **Two-layer session behavior:** repeated edge and provider sessions remain
  stable under separate affinity keys.
- **Metrics-driven provider drain:** a queue metric changes admission to
  `existing_only`; the established session stays and a new session selects the
  other provider.
- **Live overlay reload:** provider drain, withdrawal, and recovery update the
  operator-owned overlays without replacing or restarting the Praxis edge pod.
- **Edge and provider failure recovery:** an unavailable provider disappears
  from the edge view, and an unavailable edge is withdrawn behind the same
  HTTPS name before both are restored.

### Available, But Outside This Walkthrough

- **Compact deployment:** one Praxis deployment can host edge and final-hop
  provider pipelines when a separate provider trust or failure domain is not
  required.
- **Cluster-local workload ingress:** an in-cluster consumer gateway can use the
  same Grid provider-routing contract without a public GTM layer.
- **Additional provider scoring inputs:** Grid implements locality, queue,
  KV-cache utilization, prefix-cache hit ratio, latency, and cost inputs. This
  walkthrough actively changes queue admission and health rather than varying
  every scoring dimension.
- **Responses API parsing:** Praxis AI can extract models from `/v1/responses`;
  this walkthrough uses the Chat Completions request shape.
- **Authenticated GTM origin transport:** Praxis supports TLS transport
  primitives, but the local emulator uses plaintext HTTP to the edge Services
  inside the isolated demo network.

### Requires A Production Platform Or Further Work

- **Internet-scale edge steering:** production geographic, latency, weighted,
  capacity, and policy steering belongs to a managed DNS, Anycast, cloud global
  load balancer, or enterprise GSLB product. This is managed DNS/GTM, not mDNS;
  multicast DNS is unrelated to this architecture.
- **Public-edge protection:** WAF, DDoS mitigation, public certificate
  lifecycle, and Internet-facing rate controls belong to the selected ingress
  platform and its Praxis edge policy.
- **Globally shared edge affinity:** affinity shared across multiple GTM
  replicas or products requires a capability supplied by that GTM product or a
  deliberately designed shared state service.
- **In-flight stream migration:** an established streaming response is not
  moved between edges or providers after its upstream connection is created.
- **Production trust automation:** SWIM sender/origin hardening, managed key
  rotation, certificate rotation, revocation, and full readiness conditions
  remain production-hardening work.
- **Per-site replica failure proof:** this environment uses one replica for each
  role and does not claim multi-replica control-plane or data-plane availability
  within a site.
- **Production backends and credentials:** the demo intentionally uses strict
  mock inference backends and generated credentials rather than commercial
  APIs or production secrets.
