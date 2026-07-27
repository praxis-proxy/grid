# Praxis Grid Global Ingress Demo

This environment demonstrates one stable HTTPS inference endpoint backed by
two active Praxis edge gateways and two private Praxis provider gateways. Grid
discovers provider capacity, renders a local routing overlay for each edge,
and updates the running edge without restarting it.

The narrated demo is organized around four primary displays:

1. active/active global routing with independent Grid provider selection;
2. a secure provider boundary with mTLS, peer authorization, private backend
   policy, and final-hop credential replacement;
3. two-layer session affinity with metrics-driven provider drain and overlay
   hot reload; and
4. edge withdrawal, recovery, and failback behind one HTTPS name.

Every reported `PASS` is based on a runtime assertion. A manifest expressing
intent does not count as proof.

## Architecture At A Glance

The demo creates five Kind clusters. Every Praxis process runs in Kubernetes.

```text
                              client
                                |
                     https://api.grid-glb.test
                                |
                    +-----------------------+
                    | Praxis GTM emulator   |
                    | cluster: gtm-emulator |
                    +-----------+-----------+
                                |
                    +-----------+-----------+
                    |                       |
          +---------v---------+   +---------v---------+
          | Praxis east edge  |   | Praxis west edge  |
          | cluster: east-edge|   | cluster: west-edge|
          +---------+---------+   +---------+---------+
                    |                       |
                    +-----------+-----------+
                                |
                    Grid selects a provider
                                |
                    +-----------+-----------+
                    |                       |
       +------------v------------+  +-------v-----------------+
       | Praxis east provider    |  | Praxis west provider    |
       | cluster: east-provider  |  | cluster: west-provider  |
       +------------+------------+  +------------+-------------+
                    |                            |
             private backend              private backend
```

| Cluster | Praxis workload | Grid role |
|---|---|---|
| `gtm-emulator` | GTM emulator | None; local stand-in for managed global ingress |
| `east-edge` | Public edge gateway | Consumes the east edge's operator-rendered overlay |
| `east-provider` | Private provider gateway | Advertises east inference capacity |
| `west-edge` | Public edge gateway | Consumes the west edge's operator-rendered overlay |
| `west-provider` | Private provider gateway | Advertises west inference capacity |

The four edge/provider clusters each run a Grid operator and participate in
one SWIM mesh. The GTM emulator is deliberately outside that mesh because it
selects an edge, not an inference provider.

## Primary Demonstrations

| Display | What the audience sees |
|---|---|
| Active/active global and provider routing | One verified HTTPS name reaches both Praxis edges. Grid independently selects from both providers, and controlled drain or withdrawal demonstrates a crossed edge/provider path through live response attribution. |
| Secure provider boundary | Both provider gateways require edge mTLS identities, apply peer and route policy, replace the consumer credential only for the final hop, and keep the backend private behind Kubernetes NetworkPolicy. Positive and negative probes are reported together. |
| Session affinity, drain, and hot reload | Repeated requests remain on the same edge and provider. A high provider queue metric changes admission to `existing_only`: the bound session stays, a new session moves, and both edge overlays update without restarting Praxis. |
| Edge withdrawal and recovery | The east edge is scaled to zero. The same HTTPS name and edge affinity key converge to west, then return to east after Kubernetes restores the Deployment and the GTM emulator health check admits it again. |

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

### How A Request Enters An Edge Cluster

The GTM emulator does not generate inference requests. A client sends the
request to the stable HTTPS endpoint, and the emulator proxies that request to
one healthy edge:

```text
client
  |
  | HTTPS api.grid-glb.test:8443
  v
Praxis GTM emulator
  |  terminates client TLS
  |  health-checks and selects east-edge or west-edge
  |
  | HTTP on the isolated demo network
  v
selected cluster's MetalLB address
  |
  v
Service/edge-gateway:8080
  |
  | Kubernetes Service routing
  v
Praxis edge pod
  |
  | grid_route reads the edge-local overlay
  | and independently selects a provider
  v
provider-gateway:8443 over verified mTLS
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
    grid-config.json: <versioned routing overlay>
```

The local `edge-gateway` Deployment projects that ConfigMap at:

```text
/etc/grid/grid-config.json
```

Praxis AI's `grid_route` filter watches that file and reloads eligible
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
  -> Praxis grid_route hot reload
```

The overlay contains stable candidate identity, site, cluster, admission state,
selection tier, rank, and provider credential reference metadata. The edge
config maps the candidate clusters to the provider gateway LoadBalancer
addresses and requires verified upstream mTLS.

## Active/Active Global And Provider Routing

### User Story

As an application owner, I need one stable HTTPS inference endpoint backed by
active edges while Grid independently selects an admitted provider.

### Summary

Both `east-edge` and `west-edge` are active, and both edge-local Grid overlays
contain the east and west providers. The outer GTM decision and the inner Grid
decision use separate affinity keys.

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
  -> grid_route
  -> mTLS load_balancer
  -> selected provider gateway
```

`grid_route` selects from the local Grid overlay, applies admission state and
rank, preserves provider session affinity, and emits authenticated
`x-grid-peer-*` hop context for the selected provider. The following
`load_balancer` owns only transport to the selected cluster.

Example where GTM chooses the east edge and Grid chooses the west provider:

```text
client
  -> GTM emulator
  -> east public edge gateway
  -> east edge uses east Grid overlay
  -> west private provider gateway
  -> west backend
  -> west private provider gateway
  -> east public edge gateway
  -> GTM emulator
  -> client
```

The topology supports these paths:

```text
east edge -> east provider
east edge -> west provider
west edge -> east provider
west edge -> west provider
```

Normal selection remains score- and rank-driven, so the verifier does not
force all four combinations while provider state is unchanged. It proves both
active edges, proves both providers through normal routing and controlled
withdrawal, and reports any crossed edge/provider path observed in live
responses.

## Secure Provider Gateway

### User Story

As a provider owner, I need Grid traffic authenticated and authorized before
it can reach my inference backend.

### Summary

The provider Service selects a Praxis provider gateway, not the backend. The
backend is a private `ClusterIP` Service reachable only through the provider
gateway and explicitly allowed health probes.

The provider gateway uses a MetalLB address so the other local clusters can
reach it. In production this endpoint belongs on private inter-site
connectivity or an internal load balancer; mTLS and peer authorization remain
mandatory even on that private network.

### Technical Implementation

The provider pipeline is:

```text
required downstream mTLS
  -> peer_identity_trust
  -> model extraction
  -> grid_provider_route
  -> grid_credential_inject
  -> local backend load_balancer
```

Each provider:

- requires a client certificate signed by the demo CA;
- pins both edge certificate digests and the `ai-grid` organization;
- strips and validates the authenticated `x-grid-peer-*` context;
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

### Credential And Backend Isolation

### Summary

The provider gateway and backend share a Kubernetes Secret local to their
provider cluster. The gateway reads the value from a mounted file; the backend
uses the same Secret only to verify the demo request.

### Technical Implementation

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

## Session Affinity, Drain, And Hot Reload

### User Story

As an inference client, I need related requests to remain on one eligible
provider while allowing operators to drain new work without restarting the
edge.

### Summary

There are two independent affinity layers:

| Layer | Header | Decision |
|---|---|---|
| GTM emulator | `X-Edge-Session-Id` | East or west edge |
| Grid edge | `X-Session-Id` | East or west provider |

### Technical Implementation

The provider-affinity proof binds one session and repeats it. It then sets the
selected mock backend's normalized queue-depth signal above the configured
admission threshold. The provider operator scrapes that metric, derives
`existing_only`, publishes the provider state through SWIM, and the edge
operator renders the updated overlay. The proof verifies:

- the existing session remains on the selected provider;
- a new session avoids that provider;
- the running edge observes an overlay reload; and
- the edge pod UID and restart count remain unchanged.

The verifier then scales the selected provider backend to zero. Provider
health reconciliation marks the `InferenceProvider` unavailable, SWIM carries
that state to the edge operators, and the selected provider disappears from
the generated edge overlay. The running Praxis edge reloads the projected
overlay and routes a new request through the remaining provider.

Cleanup restores the backend replica count and queue metric, requires the
provider to return to `Available`, requires both edge candidates to return to
`new_and_existing`, and confirms another live reload. The verifier does not
patch operator-owned overlay or status resources.

## Failure Scenarios

### Edge Withdrawal

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

### Provider Drain

```text
mock queue metric rises above 0.85
  -> provider operator derives existing_only
  -> SWIM distributes provider state
  -> edge operator renders the admission change
  -> Praxis reloads without restarting

existing session -> selected provider remains eligible for existing work
new session      -> alternate admitted provider
```

### Provider Unavailability

```text
selected backend becomes unavailable
  -> InferenceProvider phase becomes Unavailable
  -> SWIM distributes the health change
  -> edge operator removes the candidate
  -> Praxis reloads and routes through the remaining provider

recovery restores the backend, provider phase, candidate, and live edge view
```

### Invalid Provider Peer

```text
untrusted client
  -> provider TLS/peer boundary
  -> rejected
  -> private backend never receives the request
```

## Run The Demo

### Quickstart

Clone Grid, select published immutable image references, and run the complete
setup plus narration:

```bash
git clone https://github.com/praxis-proxy/grid.git
cd grid

export GRID_XTASK_GATEWAY_IMAGE=ghcr.io/nerdalert/praxis-ai@sha256:10764e2c90af69b3a0dcffe98265da455705ce1f32aff6111a6bce3062f9319e
export GRID_XTASK_OPERATOR_IMAGE=ghcr.io/nerdalert/grid-operator@sha256:b5e42b381b62fec4bf2ec1f208d220816133e32c3f405fbdec85145915c11e06
export GRID_XTASK_MOCK_PROVIDER_IMAGE=ghcr.io/nerdalert/grid-mock-providers@sha256:8c10b74553a83a51af8fc3316f616697c4fd9b28eabe019c61d372989cb18839
export GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent

cargo xtask env run-grid-glb-demo \
  --forge-config environments/grid-glb-demo/forge.yaml \
  2>&1 | tee grid-glb-demo-output.txt
```

These temporary integration images are published under `ghcr.io/nerdalert`
until equivalent project images are available under
`ghcr.io/praxis-proxy`. The three digest references are mutually compatible.
The command builds the Rust orchestration tools locally, creates five
single-node Kind clusters, pulls the declared images, deploys the environment,
and runs all four primary displays. A non-zero exit means at least one runtime
assertion failed; the complete narration remains in
`grid-glb-demo-output.txt`.

Rerun only the narration without recreating the environment:

```bash
cargo xtask env demonstrate-grid-glb \
  --forge-config environments/grid-glb-demo/.forge.resolved.yaml \
  2>&1 | tee grid-glb-demo-rerun.txt
```

Remove the five clusters:

```bash
cargo run -p forge -- \
  --config environments/grid-glb-demo/.forge.resolved.yaml \
  --non-interactive down --force
```

### Prerequisites

- Linux with Docker;
- Kind, kubectl, curl, and OpenSSL on `PATH`;
- Rust and the repository-pinned nightly toolchain;
- capacity for five single-node Kind clusters; and
- either the three local demo images or accessible registry images.

Validate the declarative environment:

```bash
cargo run -p forge -- \
  config validate \
  --config environments/grid-glb-demo/forge.yaml
```

### Registry Images

Use published Praxis project images with immutable tags or digests:

```bash
export GRID_XTASK_GATEWAY_IMAGE=ghcr.io/praxis-proxy/praxis-ai:<tag-or-digest>
export GRID_XTASK_OPERATOR_IMAGE=ghcr.io/praxis-proxy/grid-operator:<tag-or-digest>
export GRID_XTASK_MOCK_PROVIDER_IMAGE=ghcr.io/praxis-proxy/grid-mock-providers:<tag-or-digest>
export GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent
```

`GRID_XTASK_GATEWAY_IMAGE` must include `grid_route`,
`grid_provider_route`, `grid_credential_inject`, hot reload, downstream mTLS,
upstream mTLS, and peer identity trust. The legacy mock-EPP image is not used
by this demo.

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

### One-Command Setup And Narration

```bash
cargo xtask env run-grid-glb-demo 2>&1 | tee grid-glb-demo-output.txt
```

The command:

1. resolves image overrides without changing source manifests;
2. generates distinct edge, provider, untrusted-peer, and public-name identities;
3. creates all five Kind clusters on one cross-cluster network;
4. loads local images only when pull policy is `Never`;
5. installs MetalLB in all five clusters;
6. installs Grid and the four-member SWIM mesh in edge/provider clusters;
7. installs Kubernetes TLS and credential Secrets;
8. deploys both private provider paths and captures their gateway addresses;
9. deploys both edge sites and mounts each operator-rendered overlay;
10. deploys the GTM emulator after both edge addresses are known;
11. runs the Grid routing and provider-boundary proof;
12. discovers both active edge paths and proves two-layer affinity; and
13. runs Kubernetes edge withdrawal, recovery, and failback.

Setup and narration can be run separately:

```bash
cargo xtask env setup-grid-glb

cargo xtask env demonstrate-grid-glb \
  --forge-config environments/grid-glb-demo/.forge.resolved.yaml \
  2>&1 | tee grid-glb-demo-output.txt
```

## Grid Routing And Provider-Boundary Proof

The focused Grid proof reports assertions by capability rather than exposing a
fixed step count:

| Capability group | Runtime assertions |
|---|---|
| Environment identity | Forge configuration is valid and all five declared clusters are live |
| Grid mesh | Four SWIM LoadBalancer Services exist, advertised addresses match, and every GridNetwork has the other three seeds |
| Edge overlays | Both edge-local ConfigMaps exist, identify the correct local edge, contain both provider candidates, and are projected into the matching Praxis pod |
| Provider discovery | Both provider gateway addresses match their Services and the remote GridSite egress values propagated through SWIM |
| Provider workload boundary | Provider Services select Praxis pods on `8443`; backends are `ClusterIP`; labeled and unlabeled probes prove NetworkPolicy enforcement |
| TLS and peer trust | Both edge identities succeed; missing certificate, wrong CA, wrong SNI, wrong organization, and untrusted digest fail |
| Provider-local policy | Unknown candidate, unsupported model, and unsupported path are rejected after peer authentication |
| Credential boundary | The backend rejects the consumer-supplied `Authorization` credential; the provider gateway replaces it with a provider-local credential for the successful final hop |
| End-to-end routing | A direct edge request returns HTTP 200 with matching edge, provider-gateway, backend-provider, and backend-request attribution |
| Provider session behavior | Initial binding and repeated reuse are stable; existing work survives `existing_only` while new work selects the alternate provider |
| Hot reload | Provider health withdrawal removes one candidate through normal reconciliation and SWIM propagation; the running edge reloads, routes through the remaining provider, then reloads the restored two-provider view without changing pod UID or restart count |

This proof covers Grid's provider-routing path. The separate GTM emulator proof
covers the stable public name, both edge identities, edge affinity, edge
withdrawal, recovery, and failback.

Run the focused verifiers:

```bash
cargo xtask env verify-grid-glb-routing \
  --forge-config environments/grid-glb-demo/.forge.resolved.yaml

cargo xtask env verify-grid-glb-gtm-emulator \
  --forge-config environments/grid-glb-demo/.forge.resolved.yaml
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

### Demo-Only Attribution Headers

The demo stamps response headers that identify which edge and provider served
each request. These headers exist solely so that the verifier can make runtime
assertions about routing behavior. Without them, requests through different
edges or providers return identical response bodies, and no client-side test
could prove active/active routing, session stickiness, or withdrawal/recovery.

```text
X-Grid-Demo-Edge-Gateway            which edge cluster handled the request
X-Grid-Demo-Provider-Gateway        which provider gateway forwarded it
X-Grid-Demo-Provider                which backend cluster produced the response
X-Grid-Demo-Backend-Provider-Attribution  backend-side provider identity
X-Grid-Demo-Backend-Request-Id      unique request identifier from the backend
```

These headers are **not part of Praxis or Grid production code**. They are
configured entirely within this demo environment:

- The edge header is a `headers` filter entry in
  `configs/edge/praxis.yaml` — a standard Praxis response-header injection,
  not a Grid-specific mechanism.
- The provider and backend headers are set by the mock-inference server in
  `mock-providers/`, which is a test binary that does not ship in production.

No Praxis filter, Grid operator, or production crate references these header
names. Removing this demo environment removes every trace of them. A production
deployment would use distributed tracing or access logs for path attribution,
not response headers.

## Repository Layout

| Area | Location |
|---|---|
| Environment orchestration | `forge.yaml` |
| Edge Praxis pipeline | `configs/edge/praxis.yaml` |
| Provider Praxis pipelines | `configs/east-provider/`, `configs/west-provider/` |
| GTM emulator Praxis pipeline | `configs/gtm-emulator/praxis.yaml` |
| GridNetwork and GridSite resources | `resources/gridnetwork-*.yaml`, `resources/site-*.yaml` |
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
| [praxis-proxy/ai#339](https://github.com/praxis-proxy/ai/pull/339) | `grid_route`, provider-hop context, and selected-candidate routing | Base Grid routing capability |
| [praxis-proxy/ai#540](https://github.com/praxis-proxy/ai/pull/540) | Overlay-file hot reload | Builds on `grid_route` |
| [praxis-proxy/ai#386](https://github.com/praxis-proxy/ai/pull/386) | Provider-local route validation and credential injection | Consumes authenticated provider-hop context |

The three PRs remain independently owned even when a temporary integration
image combines them for end-to-end validation.

## Demonstrated And Non-Demonstrated Boundaries

Demonstrated:

- one verified HTTPS name over two active Praxis edges;
- Kubernetes-native edge and provider deployments;
- four-cluster Grid discovery and two edge-local overlays;
- both active edges, both providers, and a controlled crossed-region path;
- provider mTLS, peer authorization, and exact local route mapping;
- final-hop credential replacement and private backend policy;
- edge and provider session affinity;
- provider drain and overlay hot reload without edge restart; and
- edge withdrawal, recovery, and failback.

Not demonstrated:

- managed DNS or Anycast behavior;
- real Internet geo-latency or capacity-based edge steering;
- authenticated GTM-to-edge origin transport;
- production SWIM encryption and managed key rotation;
- shared GTM affinity state across emulator replicas;
- WAF, DDoS, rate-limit, or public certificate automation;
- in-flight streaming migration;
- production provider credentials or external commercial APIs; and
- multi-replica control-plane or data-plane high availability within one site.
