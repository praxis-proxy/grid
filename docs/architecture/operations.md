# Grid Operations

This guide covers production installation, grid formation, site lifecycle,
routing configuration, security, and observability. Development-only
orchestration is isolated under **Development Validation Environments**.

## 1. Deploy the Grid Operator

### Install

Grid provides a Helm chart and Kustomize manifests for the operator and CRDs.

**Option 1: Helm (recommended)**

```console
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <version> \
  --namespace grid-system \
  --create-namespace
```

To grant resource access to additional namespaces:

```console
helm upgrade grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --set "resourceNamespaces={app-ns,data-ns}" \
  --namespace grid-system
```

Helm installs CRDs on first install but does not upgrade them. When upgrading
to a version with changed CRDs, apply the new CRDs before the chart upgrade:

```console
kubectl apply -f charts/grid-operator/crds/
helm upgrade grid-operator oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <new-version> --namespace grid-system
```

Uninstalling the chart removes namespaced resources but retains CRDs.
Custom resources created by other chart releases (e.g., grid-site) are
not affected. See the [chart README](../../charts/grid-operator/README.md)
for the full values reference.

**Option 2: Kustomize**

```console
# Complete Grid deployment (CRDs + operator)
kubectl apply -k deploy/

# Or step-by-step:
kubectl apply -f deploy/crds/
kubectl apply -k deploy/operator/
```

**Option 2: Direct YAML**

```console
# Apply CRDs first
kubectl apply -f deploy/crds/gridnetwork.yaml
kubectl apply -f deploy/crds/gridsite.yaml
kubectl apply -f deploy/crds/inferenceprovider.yaml

# Apply operator resources with Kustomize
kubectl apply -k deploy/operator/
```

**Option 3: Generated CRDs (development)**

```console
# Generate CRDs from source and apply directly
cargo run -p operator --bin generate_crds | kubectl apply -f -
kubectl apply -k deploy/operator/
```

For regenerating CRDs after schema changes:

```console
./scripts/generate-deployment-crds.sh
```

**Container image pattern**: The operator Containerfile uses a
`rust:1.96-alpine` builder, dependency-cache stubs, and an `alpine:3.23`
runtime with a non-root user and no build toolchain.  The Kubernetes Deployment
adds a restricted security context for OpenShift-style clusters.

**Image availability**: Production deployments pin project-owned release or
commit-digest images. Local image loading is documented under
**Development Validation Environments**.

### Deployment Examples

See sample Custom Resource configurations:
- `config/samples/` - standard operator sample CRs
- `deploy/examples/single-cluster-api-provider/` - minimal example with external API

The install package creates:

| Resource | Name | Scope |
|---|---|---|
| `Namespace` | `grid-system` | cluster |
| `ServiceAccount` | `grid-operator` | `grid-system` |
| `ClusterRole` | `grid-operator-crd` | cluster |
| `ClusterRoleBinding` | `grid-operator-crd` | cluster |
| `ClusterRole` | `grid-operator-resources` | cluster (verb definitions only) |
| `RoleBinding` | `grid-operator-resources` | `default` namespace |
| `Deployment` | `grid-operator` | `grid-system` |

The operator runs as a single binary with multiple
controllers (one per CRD type) in the same process.  No
SWIM runtime starts until a `GridNetwork` resource exists.

**Important**: Grid deploys only the operator and CRDs. Cluster lifecycle,
Praxis AI gateways, inference runtimes, load-balancer integrations, and DNS
remain deployment-platform responsibilities.

Praxis AI gateway deployment is separate and requires:
1. Praxis AI image with required filters (`intelligent_route`, `credential_inject`)
2. Consumer gateway configuration referencing Grid-generated ConfigMaps
3. Provider gateway deployment with Grid-compatible endpoints

### Operator image

The intended project-owned operator image path is:

```
ghcr.io/praxis-proxy/grid-operator
```

Repository CI publishes source-SHA images, and the release workflow publishes
versioned images with SBOM and provenance attestations.

**Tag policy:**

| Tag | Mutability | When pushed |
|---|---|---|
| `sha-<7-char-commit>` | Immutable | Used once publishing is enabled |
| `v<version>` | Immutable release tag | Used once releases are cut |

Deployments should pin an immutable digest:

```yaml
image: ghcr.io/praxis-proxy/grid-operator@sha256:8c8271aa589fbd81e346b75ae580be9e8085c3b283b4e6a99e2b9adcea73e12d
```

**Override:** replace the `image:` field in
`deploy/operator/deployment.yaml` or patch the
Deployment after apply:

```console
kubectl set image deployment/grid-operator \
  -n grid-system \
  operator=ghcr.io/praxis-proxy/grid-operator:sha-<commit>
```

**Registry namespace:** the image is published under `ghcr.io/praxis-proxy/`.

**CI publishing setup:** the
`.github/workflows/operator-image.yaml` publishes source-SHA images from
`main`. `.github/workflows/release.yaml` publishes version and source-SHA tags
after the tagged source passes release gates.

**Security:** the operator image contains only the
statically linked operator binary.  No secrets, tokens,
SWIM encryption keys, or credentials are baked into the
image. Production publication requires an SBOM and image
signing in the release pipeline.

### RBAC permissions

RBAC is split into two `ClusterRoles`:

1. **`grid-operator-crd`** — cluster-scoped CRD access,
   bound via a `ClusterRoleBinding`.
2. **`grid-operator-resources`** — namespaced `Secret` and
   `ConfigMap` access, bound via per-namespace
   `RoleBindings`.

The default install includes a `RoleBinding` in the
`default` namespace only.  All mutations use server-side
apply (`patch`).  SSA on a non-existent resource requires
`create` permission, so both `create` and `patch` are
granted for `secrets` and `configmaps`.  `delete` and
`update` are not granted.

**Grid CRDs (cluster-scoped, `grid-operator-crd`):**

| Resource | Verbs | Why |
|---|---|---|
| `gridnetworks` | `get`, `list`, `watch`, `patch` | Controller watch loop; SSA spec/status writes |
| `gridnetworks/status` | `get`, `patch` | Phase, connectedSites, distributedProviderCount |
| `gridsites` | `get`, `list`, `watch`, `patch` | Controller watch; auto-creation from SWIM Alive members |
| `gridsites/status` | `get`, `patch` | Phase, reason, publicCertPem, observedGeneration |
| `inferenceproviders` | `get`, `list`, `watch`, `patch` | Controller watch; site-selector matching |
| `inferenceproviders/status` | `get`, `patch` | Phase, matchingSites, observedGeneration |

**Events (`events.k8s.io`, `grid-operator-resources`):**

| Resource | Verbs | Why |
|---|---|---|
| `events` | `create`, `patch` | Published on `GridSite` phase/reason transitions with action `GatewayProbe` |

**Core resources (namespaced, `grid-operator-resources`):**

| Resource | Verbs | Why |
|---|---|---|
| `secrets` | `get`, `create`, `patch` | Read TLS certs, SWIM key, credential refs; SSA-create CA and site cert `Secrets` |
| `configmaps` | `create`, `patch` | SSA-create routing overlay and consumer config `ConfigMaps` |

The `grid-operator-resources` `ClusterRole` is never bound
cluster-wide.  It takes effect only in namespaces where a
`RoleBinding` references it.

### Secret access rules

The operator reads `Secrets` in the namespace declared by
each `SecretRef` in the CRD spec.  It does not search
across namespaces or list `Secrets`.

| Secret path | Keys read | Keys written |
|---|---|---|
| `spec.tls.siteSecretRef` | `tls.crt`, `tls.key` (client cert + private key for mTLS gateway probes; key bytes wrapped in `Zeroizing`) | `tls.crt`, `tls.key` (create-if-absent via SSA patch) |
| `spec.tls.caSecretRef` | `ca.crt` (existence check) | `ca.crt`, `ca.key` (create-if-absent via SSA patch) |
| `spec.tls.swimKeyRef` | `key` (or custom key field) | — |
| `spec.auth.secretRef` | existence + UTF-8 validation | — |

Secret writes use SSA `patch` with field manager
`grid-operator`.  SSA on a non-existent resource requires
both `create` and `patch` permission in the target
namespace.

Credential token bytes are never written to `ConfigMaps`,
overlays, status fields, or logs.

### ConfigMap write scope

| `ConfigMap` | Naming | Data key | Namespace |
|---|---|---|---|
| Routing overlay | `grid-overlay-{network}-{gateway}` | `routing-overlay.json`, `routing-config.json` | `GatewayRef.namespace` |
| Consumer config | `consumerConfig.configMapName` | `praxis.yaml` | `GatewayRef.namespace` |

### What is not granted

Neither `ClusterRole` grants:

- `pods`, `pods/exec`, `pods/log`, `pods/portforward`
- `deployments`, `services`, `ingresses`
- `secrets` `delete`, `list`, `watch`
- `configmaps` `get`, `delete`, `list`, `watch`
- Any `update` verb (all mutations use SSA `patch`)

### Adding namespaces

The default install grants `Secret` and `ConfigMap` access
only in the `default` namespace.  To grant access in
additional namespaces (e.g. the gateway namespace
referenced by `GatewayRef`, or the namespace holding TLS
`Secrets`), create a `RoleBinding` in each:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: grid-operator-resources
  namespace: praxis-system          # the target namespace
subjects:
  - kind: ServiceAccount
    name: grid-operator
    namespace: grid-system
roleRef:
  kind: ClusterRole
  name: grid-operator-resources
  apiGroup: rbac.authorization.k8s.io
```

Add one `RoleBinding` per namespace referenced by
`GatewayRef.namespace`, `tls.caSecretRef.namespace`,
`tls.siteSecretRef.namespace`, `tls.swimKeyRef.namespace`,
and `auth.secretRef.namespace` in your CRD specs.

### Deployment configuration

The `Deployment` in `deploy/operator/deployment.yaml`
exposes SWIM configuration through environment variables:

| Variable | Purpose |
|---|---|
| `GRID_SWIM_BIND_ADDR` | UDP address to bind the SWIM listener |
| `GRID_SWIM_ADVERTISE_ADDR` | Address advertised to peers (defaults to `$(POD_IP):7946`) |
| `GRID_SWIM_SITE_NAME` | Unique site identity for this operator instance |
| `GRID_SWIM_SEEDS` | Comma-separated SWIM seed addresses |
| `GRID_GATEWAY_ADDRESS` | Explicit gateway address override (skips self-discovery) |
| `GRID_GATEWAY_SERVICE_NAME` | Service name for gateway self-discovery (default: `provider-gateway`) |
| `GRID_GATEWAY_NAMESPACE` | Namespace for gateway Service lookup (default: `grid-system`) |
| `GRID_GATEWAY_PORT` | Port appended to discovered address (default: `8080`) |
| `GRID_GATEWAY_DISCOVERY_INTERVAL_MS` | Polling interval for gateway discovery (default: `5000`) |

`GRID_SWIM_ENCRYPT_KEY` is intentionally omitted from the
`Deployment`.  Production SWIM encryption uses
`GridNetwork.spec.tls.swimKeyRef` to reference a
Kubernetes `Secret`.  The env var exists for local
development and testing only.

### Validate the install

```console
cargo xtask env verify-operator-install-rbac \
  -c tests/env/operator-routing.toml
```

This command builds the operator image, loads it into a
Kind cluster, applies the install manifests, runs positive
and negative `kubectl auth can-i` checks (including
namespace-scope proofs), then waits for the
in-cluster `Deployment` to reconcile a test `GridNetwork`
using only the installed `ServiceAccount`.

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
4. Starts the SWIM runtime with seed peers
5. Sets `status.phase: Initializing`

### CRD-driven seeds

`spec.seeds` is **operator-consumed**: on every `GridNetwork` reconcile the
controller parses the seed list, filters invalid addresses (logged at `warn`,
no reconcile failure), removes the local advertise address to prevent self-
announce noise, deduplicates, and calls `SwimHandle::announce_seeds` to deliver
the batch to the running SWIM event loop.  Re-announcing to already-connected
peers is idempotent — foca ignores redundant joins.

Startup seeds from `GRID_SWIM_SEEDS` (env var) and CRD seeds are additive.
The env var seeds are applied once at startup; CRD seeds are applied on every
reconcile, so dynamically added addresses take effect without an operator restart.

**Runtime update contract**

| Change to `spec.seeds` | Effect |
|---|---|
| Seed added | Announced to SWIM on the next reconcile; join initiated |
| Seed removed | Logged; no active disconnect — SWIM failure detection ages the peer out naturally |
| Seeds unchanged | Re-announced idempotently; no side effects |

Adding a seed requires no operator restart.  The new address is SWIM-joined within
one reconcile cycle (~300 s default requeue, or sooner if a watch event fires).

Removing a seed does not disconnect the peer.  The removed peer remains in SWIM
membership until it stops responding to probes and is declared `Suspect` then
`Dead` by the SWIM protocol.

**Global-runtime semantics**

The SWIM runtime is process-global — one UDP listener per operator process,
shared across all `GridNetwork` reconciles.  Seeds from any
`GridNetwork.spec.seeds` are announced to the same SWIM membership node.
This is site-membership bootstrap, not per-network membership isolation.
CRDT provider records remain network-scoped separately.

Each membership identity carries a restart-scale `u64` generation. A
replacement process at the same advertised address presents a greater
generation and supersedes the retained process identity; in-process renewals
increment it without wrapping. All peers in one SWIM membership domain must
run a wire-compatible identity schema. A release that changes that schema
must be coordinated across the membership domain rather than deployed as an
unqualified rolling update.

**Transport-security contract**

SWIM is the Grid control-plane membership and state broadcast channel.  When
`spec.tls.swimKeyRef` is configured and the referenced Secret resolves to a
valid 32-byte key, reconcile applies the key before announcing CRD seeds or
publishing certificate/provider state.  From that point, outgoing SWIM UDP
packets are encrypted and authenticated with AES-256-GCM.  Incoming packets
that fail authentication are silently dropped; the foca membership state
machine never sees them.

When `swimKeyRef` is absent, SWIM traffic is sent and received as cleartext
(backward-compatible local and development behavior).

If `swimKeyRef` is configured but the Secret is missing, unreadable, or not a
valid 32-byte key, the reconcile fails before CRD seed announcement and
certificate/provider broadcasts for that `GridNetwork`.  The SWIM runtime is
process-global, so a previously loaded key remains active until restart; the
operator does not switch to plaintext for that configured reconcile.

`GRID_SWIM_ENCRYPT_KEY` is the local and Kind validation path for startup-time
enforcement because it is available before the UDP socket starts.  It is
process environment material and should not be treated as the production Secret
delivery mechanism.  With CRD-backed `swimKeyRef`, the key is applied at
`GridNetwork` reconcile time; use the environment key as well when startup-time
plaintext acceptance must be avoided before CRD preload support exists.

**SWIM encryption protects:** gossip membership packets, gateway address
broadcasts, public certificate PEM broadcasts, and CRDT provider state broadcasts.

**SWIM encryption does not protect:** data-plane request traffic.  Gateway
request-time authentication and authorization are enforced by Praxis/Praxis AI
gateway TLS and peer identity filters, not by SWIM membership.

Routing eligibility remains fail-closed at the `GridSite` layer independently of
SWIM encryption: remote CRDT provider records are rendered only for peers whose
`GridSite` is `Active`. Active indicates control-plane eligibility — sufficient
trust information to include the site in routing overlays. Data-plane readiness
is verified separately. Both layers are required for production deployments.

**Channel-full retry**

If the seed announce channel is full (capacity 16 batches), the announce is
skipped for the current reconcile and retried on the next
(`REQUEUE_INTERVAL = 300 s`).  Seeds are not guaranteed to be applied
immediately under heavy broadcast load.

**Seed format**
Seeds must be `IP:port` socket addresses.  DNS names are not resolved.
Example: `10.0.0.2:7946`.  Invalid addresses are skipped and logged at `warn`
level; the reconcile does not fail.

**Troubleshooting seed changes**

*New seed not joining:*
- Verify the address is a valid `IP:port`.
- Check the operator log for `announcing CRD seeds to SWIM runtime` or
  `new CRD seeds added` — if absent, the reconcile may not have fired yet.
- Check for `failed to queue CRD seeds for SWIM announcement` at `warn` level,
  indicating a channel-full retry.
- Verify the remote operator is running with `GRID_SWIM_BIND_ADDR` set to the
  expected address.

*Removed seed still shows as connected:*
- Expected behavior.  SWIM does not actively disconnect on seed removal.
- Wait for the WAN probe and suspicion window. The runtime probes every five
  seconds, allows three seconds for a direct response before indirect probes,
  and retains a suspect member for at least three probe periods before
  declaring it `Dead`. `GridNetwork.status.connectedSites` then decreases.
- If the remote operator is still running, it will rejoin as `Alive` again because
  SWIM membership is peer-to-peer and periodically announces to live and down
  members. Seeds bootstrap discovery; they are not an allowlist.

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

## 4. Gateway Address and Trust Bootstrap

SWIM discovery proves that a peer is participating in gossip.  It does not
authorize that peer for request routing.

### SWIM bootstrap phases

The trust bootstrap for a remote site progresses through these steps:

1. **SWIM discovery** — the peer is observed as Alive in SWIM membership.
   Phase: `Discovered`.  No trust established.

2. **Gateway address known** — the remote operator advertises its resolved gateway
   address via SWIM state broadcast.  The address is resolved by the self-discovery
   poller (Service LoadBalancer lookup) or from the `GRID_GATEWAY_ADDRESS` override.
   The local operator stores it in `GridSite.spec.egress.address`.
   Phase: `Connecting`.  No trust established.

3. **Public cert material received** — the remote operator broadcasts its public site
   certificate PEM.  The operator validates the PEM structure (rejects private-key markers;
   checks for `CERTIFICATE` header) and stores it in `GridSite.status.publicCertPem`.

4. **Identity policy configured** — set `spec.egress.tls.serverName` to the
   expected DNS SAN and set `spec.trust.canonicalFingerprints` to one or two
   independently verified DER-certificate SHA-256 pins. Configure
   `GridNetwork.spec.tls.caSecretRef` and `siteSecretRef` for server and client
   authentication.

5. **Identity-aware gateway probe passes** — the `GridSite` controller performs
   a bounded mTLS handshake. It verifies the CA chain, DNS SAN, client
   authentication, canonical pin, and agreement with the SWIM-advertised leaf
   certificate. Success promotes the site to `Active` with reason
   `TlsVerified`.

   ```yaml
   spec:
     egress:
       address: provider.example.com:8443
       tls:
         mode: Mutual
         serverName: provider.example.com
     trust:
       canonicalFingerprints:
         - "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
   ```

   See [Authentication and Access Policy](auth.md) for the trust contract.

6. **Data-plane mTLS enforced** — a provider Praxis gateway validates peer
   identity over mTLS on every request, independent of the control-plane
   phase. Deployment acceptance requires positive and negative runtime probes;
   manifest inspection alone is not evidence.

### Authentication vs authorization

| Concept | Question it answers | Grid mechanism |
|---|---|---|
| Authentication | "Is this peer really the site it claims to be?" | Identity-aware Grid health probe plus gateway mTLS peer validation on each request |
| Authorization | "Is this authenticated peer allowed to participate in this Grid or receive/send this traffic?" | Local Grid policy plus destination gateway enforcement |

A peer must satisfy both.  A SWIM peer must never become routable solely because
it gossiped successfully.

### Security rules

- SWIM membership is discovery, not authorization.
- TCP reachability proves an address accepts connections, not identity.
- `publicCertPem` present means the PEM structure is valid and no private-key markers
  were detected.  It does not prove the cert is signed by a trusted CA or that
  the peer is authorized.
- Private keys, credential tokens, and Secret data must never be written to
  `GridSite` status, `GridNetwork` status, overlays, generated ConfigMaps, or logs.
- The operator does not copy Kubernetes Secrets across clusters as part of site discovery.
- The provider gateway still enforces peer identity on every request with mTLS,
  independently of `publicCertPem` status.

### Routing eligibility

`GridSite.status.phase == Active` is the control-plane eligibility gate for remote CRDT provider records.
Provider records advertised by a SWIM peer are included in the routing overlay only when
the corresponding `GridSite` is `Active`.  Peers in `Discovered`, `Connecting`, or any
other phase are excluded.  Peers with no matching `GridSite` are also excluded (fail-closed).

GridSite Active is a control-plane eligibility signal. It proves the configured
gateway health probe succeeded. It does not prove that Praxis loaded the latest
routing config or authorized a particular request.

Setting `Active` in Mutual mode requires the configured CA, client identity,
server name, canonical pin, and live gateway certificate to agree. A provider
gateway independently authorizes peer identity on every data-plane request.
`Active` alone is not evidence that request authorization succeeded.

## 5. Connectivity Verification

The `GridSite` controller verifies gateway reachability and identity against
`spec.egress.address`.

| Condition | Current check |
|-----------|---------------|
| `SWIMReachable` | SWIM membership reports the peer Alive |
| `GatewayAddressKnown` | `spec.egress.address` is non-empty |
| `TlsVerified` | Mutual TLS handshake, chain, SAN, pin, and advertised leaf all verify |
| `IdentityVerificationRequired` | Plaintext endpoint accepts TCP, but remains ineligible because its identity is not verified |

Request-time authorization remains enforced by the provider gateway after the
control-plane health evaluation succeeds.

## 6. Capability Negotiation

Sites publish capability and provider state through Grid control-plane records
and CRDT-over-SWIM propagation.  Capability information can include models,
tools, agents, and provider availability signals.

The `GridSite` status `capabilities` field records broad site capability
classes.  A site should only be treated as fully usable after discovery,
gateway reachability, trust establishment, and data-plane readiness are all
satisfied.

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

- **`routing-overlay.json`**: the versioned, content-addressed envelope consumed
  by Praxis AI. It includes scope, provenance, revision, digest, and the routing
  payload.
- **`routing-config.json`**: JSON-serialised
  legacy `RoutingOverlay` payload with one `RoutingCandidate` per
  model per `InferenceProvider` in the network.  When
  `spec.auth.secretRef` is set, candidates carry only
  the credential reference, never token bytes.

The overlay shape is compatible with the Praxis
`intelligent_route` filter:

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

### Routing overlay delivery

For gateways that must react promptly to provider health, capacity, or score
changes, enable the overlay-sync delivery path in the `praxis-gateway` chart.

```text
operator reconcile
  -> overlay ConfigMap applied
  -> overlay-sync Kubernetes API watch
  -> envelope validated
  -> atomic write to shared emptyDir
  -> Praxis file watcher hot-reloads
```

A direct ConfigMap volume is eventually refreshed by the kubelet. That is
adequate for static or slowly changing configuration, but its refresh delay is
not appropriate as the delivery mechanism for short-lived routing changes.
The sidecar watches the API directly, so a published revision does not wait for
the kubelet's projected-volume polling cycle.

The sidecar adds correctness controls as well as lower latency:

- an init container blocks Praxis startup until the first valid overlay exists;
- schema version, destination scope, content digest, and maximum size are
  checked before publication;
- a temporary file plus `fsync` and rename prevents partial reads;
- invalid updates, source deletion, and temporary API loss retain the
  last-known-good file;
- readiness, degraded state, accepted/rejected counters, write counters, and
  timestamps make the delivery boundary observable; and
- a dedicated ServiceAccount token is mounted only into the init and sidecar
  containers. Praxis has no Kubernetes API access.

The sidecar does not change the metrics scrape interval or force the Grid
operator to reconcile. End-to-end convergence is still:

```text
metrics become visible
  + operator scrape/reconcile
  + ConfigMap apply
  + sidecar API-watch delivery
  + Praxis file-watch reload
```

Use `overlay.sidecar.enabled=false` for the direct ConfigMap projection
fallback. Do not use that mode when a demo or production SLO assumes prompt
metrics-driven route changes.

## 9. Workloads Consume Providers

Workloads send requests to the Praxis Gateway.
The gateway's grid scoring filter selects the optimal
backend. Praxis AI handles API translation and credential
injection transparently.

For API-provider routes, the request-time path is:

```text
intelligent_route
  -> writes intelligent_route.credential.* metadata from the selected candidate
credential_inject
  -> reads the matching token from a mounted Secret file
  -> injects Authorization: Bearer <token>
load_balancer
  -> forwards to the selected provider cluster
```

The token is not stored in the Grid overlay or consumer
Praxis `ConfigMap`.

For direct API-provider and cloud-provider fallback, the
consumer gateway is often also the final-hop gateway, so the
credential Secret is mounted there.  For remote Grid sites,
provider credentials should live only in the remote provider
site or provider-side component that makes the final backend
call.  Grid carries the reference needed for routing and
configuration; it does not copy Secret values between
clusters.

The native path requires a Praxis AI image that includes the
`credential_inject` filter.  Grid can render the
file-backed filter config today, but runtime deployments must
use an AI image with that filter merged and published.

See [Auth & Policy](auth.md) for workload access
patterns and authentication strategies.

## GridSite trust bootstrap

### Public certificate exchange

When a `GridNetwork` has `spec.tls.siteSecretRef` configured, the operator reads
the public site certificate (`tls.crt`) from that Secret on each reconcile and
broadcasts it to SWIM peers.  Remote peers store the received certificate in
`GridSite.status.publicCertPem`.

To verify that a remote site's public certificate has been received:

```console
kubectl get gridsite <site-name> -o jsonpath='{.status.publicCertPem}'
```

A non-empty value means the remote operator is advertising structurally valid
public certificate material. To advance a Mutual TLS site to `Active`, configure
the CA and local client identity on the `GridNetwork`, then configure the
expected `serverName` and `canonicalFingerprints` on the `GridSite`.

A site in `TrustMaterialMissing` lacks at least one required CA, client
certificate, client key, server name, or canonical pin. Configure
`spec.tls.siteSecretRef` on the remote `GridNetwork` to enable certificate
advertisement, but do not derive trust solely from the advertised value.

### Security rules

The public certificate recorded in `status.publicCertPem` is **not** automatically
trusted.  The control plane records received trust material for operator visibility.
The provider gateway enforces mTLS peer identity and certificate validation on every
request — the control plane record does not bypass that check.

Private keys are never included in SWIM broadcasts.  The operator reads only the
public certificate (`tls.crt`) from the site Secret, not the private key (`tls.key`).

## GridSite gateway address configuration

The operator resolves and advertises its data-plane gateway address to SWIM
peers. This address is propagated through SWIM state broadcasts and used by
receiving operators to populate `GridSite.spec.egress.address` for
auto-discovered sites.

**Self-discovery (default):** A background poller periodically looks up the
`provider-gateway` LoadBalancer Service and extracts its external IP.  The
poller retries every 5 seconds (configurable via
`GRID_GATEWAY_DISCOVERY_INTERVAL_MS`) until the address appears, then
continues watching for changes.  Discovered addresses are pushed to the SWIM
runtime via a watch channel.

**Explicit override:** Set `GRID_GATEWAY_ADDRESS` to skip the self-discovery
poller entirely.

```bash
# Self-discovery (default): operator discovers from provider-gateway Service
GRID_GATEWAY_SERVICE_NAME=provider-gateway ./operator

# Explicit override: skip discovery poller
GRID_GATEWAY_ADDRESS=10.0.0.4:8080 ./operator
```

**Requirements:**
- Format: `host:port` or `IP:port` (any non-empty string is accepted; the remote
  operator stores it verbatim in `GridSite.spec.egress.address`)
- When absent or empty and no LoadBalancer Service exists: auto-discovered
  `GridSite` records have empty `spec.egress.address` and stay in `Discovered`
  phase until the Service appears
- This address is separate from `GRID_SWIM_BIND_ADDR` — the SWIM gossip endpoint
  and the data-plane gateway address are distinct

The current first-ingress, configured-port discovery contract is appropriate
for the local MetalLB environment. A production endpoint is represented and
validated by host, named port, protocol, SNI, address scope, and generation;
arbitrary or ambiguous advertised strings do not become routable endpoints.

**Probe behavior:** In Mutual mode, the `GridSite` controller performs a bounded
mTLS connection to `spec.egress.address`. It verifies the configured CA,
`serverName`, canonical live-certificate pin, and that any SWIM-advertised
certificate matches one of the configured rotation pins. A successful probe
reports `reason: TlsVerified`. Connection failures move an Active site to
`Unreachable`; identity or trust failures move it to `Connecting`.

Explicit `Plaintext` mode performs only a bounded TCP connection for
diagnostics. It never promotes a site to `Active` and is never selected as a
fallback when Mutual TLS configuration is incomplete or invalid.

## GridSite Lifecycle Diagnostics

Use `kubectl get gridsites` to inspect current lifecycle phases:

```console
kubectl get gridsites
```

Example output:

```
NAME                              PHASE        NETWORK
op-e2e-sjd-net-grid-site-b       Connecting   op-e2e-sjd-net
```

To see the reason and diagnostic message:

```console
kubectl get gridsite <name> -o jsonpath='{.status.phase}/{.status.reason}: {.status.message}'
```

### Phase transitions and their cause

| From | To | Trigger |
|---|---|---|
| (new) | Pending | Resource created |
| Pending | Discovered | `GridNetwork` controller observes SWIM Alive member |
| Discovered | Connecting | `GridSite` controller: `spec.egress.address` non-empty |
| Connecting | Active | `GridSite` controller: configured Mutual TLS identity probe succeeds |
| Active | Connecting | TLS identity or trust verification fails, or the endpoint is changed to plaintext |
| Active | Unreachable | Gateway address is missing, times out, or refuses the connection |

Security invariant: a SWIM peer must never become routable solely because it
gossiped successfully.  Discovery, authentication, and authorization are
separate steps.

### Troubleshooting

**Phase stays Pending after SWIM convergence**

- Check the `GridNetwork` has the label `grid.praxis-proxy.io/auto-discover-sites: "true"`.
- Check that the `GridNetwork` controller has SWIM running (`GRID_SWIM_BIND_ADDR` env var set).
- Check `kubectl get gridnetwork <name> -o jsonpath='{.status.connectedSites}'` — must be > 0.

**Phase stays Discovered (not advancing to Connecting)**

- The site has no `spec.egress.address`.  Verify the remote operator's
  `provider-gateway` LoadBalancer Service exists and has an external IP assigned,
  or set `GRID_GATEWAY_ADDRESS` as an explicit override.  The self-discovery poller
  will propagate the address through SWIM once discovered.
- Reason will be `GatewayAddressMissing`.

**Phase stays Connecting**

- Check `status.reason`:
  - `TrustMaterialMissing`: configure the CA Secret, local client identity,
    `serverName`, and canonical pin policy.
  - `TrustMaterialInvalid`: trust material is malformed, oversized, or uses the
    deprecated PEM fingerprint field.
  - `UntrustedIssuer`, `IdentityMismatch`, `CertificateExpired`, or
    `CertificateNotYetValid`: inspect the live gateway certificate and
    configured CA/server name.
  - `PinMismatch`: the live leaf certificate does not match either configured
    canonical pin.
  - `AdvertisedCertMismatch`: wait for certificate gossip to converge or
    investigate an unexpected live gateway identity.
  - `HandshakeTimeout` or `TlsProtocolError`: the TCP endpoint answered but did
    not complete the expected TLS protocol.

**Phase is Active, site became Unreachable**

- The connection to `spec.egress.address` failed. When connectivity returns,
  the complete configured identity probe must pass before the site returns to
  Active.

**RBAC for GridSite status updates**

The `GridSite` and `GridNetwork` controllers both write to `GridSite` status.
The `grid-operator-crd` `ClusterRole` in `deploy/operator/cluster-role-crd.yaml`
includes `gridsites/status` with verbs `get` and `patch`.

## Consumer Config

When `GatewayRef.consumerConfig.enabled: true`, the Grid operator applies a
`ConfigMap` in the gateway's namespace on every reconcile.  The
`grid-operator-resources` `ClusterRole` includes `configmaps` with verbs
`create` and `patch`.  A `RoleBinding` in the gateway's namespace is required
for the operator `ServiceAccount` to write the `ConfigMap` there.

Every `clusterEndpoints[]` entry must declare explicit transport intent via the
`transport` field.  Remote/provider-gateway clusters should use
`transport.mode: mutual_tls` with a non-blank `transport.sni` matching the
provider certificate SAN.  Local dev-only clusters may use
`transport.mode: plaintext`.  Missing transport fails closed with status reason
`MissingTransport`; missing SNI on `mutual_tls` fails with `MissingSni`.
`transport.mode` is the security switch — not the presence of `sni`.

### Credential Secret access

The generated `ConfigMap` references credential Secrets by name, namespace, and
key — it does not read Secret values.  The operator does NOT require `get` access
to credential Secrets in the gateway namespace for config generation.

The final-hop gateway or provider-side component making the final backend call
needs the credential Secret mounted.  Secret provisioning in that cluster is
the responsibility of external tooling (platform automation, External Secrets,
Vault, or a manual process).  The Grid operator does not copy Secrets across
clusters.

### Cross-cluster limitations

The operator's RBAC controls access within its own cluster.  When the consumer
gateway runs in a different cluster, the generated `ConfigMap` must be delivered
externally — the operator cannot write to a remote cluster's API server directly.
The Kind validation harness (`verify-api-fallback-native`) bridges this gap for
local testing by reading the generated YAML and re-applying it as
`praxis-consumer-config` in the consumer cluster.  Production cross-cluster
delivery requires GitOps, External Secrets, or a similar mechanism.

## Site Departure

The running SWIM implementation detects an abrupt loss through direct and
indirect probes. Remote provider records are treated as degraded when the peer
is `Suspect` or `Dead`, and stale-candidate retention follows the configured
overlay TTL policy.

The current operator does not delete the `GridSite`, garbage-collect the CRDT
record, or automatically complete a `Left` transition on process shutdown.
Departure therefore preserves control-plane evidence and requires explicit
site lifecycle cleanup by the deployment owner.

## Adding a New Site to an Existing Grid

1. Deploy the Grid Operator on the new cluster
2. Create a `GridNetwork` with any existing cluster
   as a seed
3. SWIM discovers the existing cluster, which shares
   the membership list of all other sites
4. The new site automatically discovers all grid
   members within seconds
5. SWIM propagates public certificate material; the operator
   verifies the explicitly configured fingerprint and advances matching sites
   to `Active`
6. Once `Active`, the new site's providers are visible
   to all other sites through the routing overlay

## External Ingress Operations

External ingress operates as a two-stage service:

```text
managed GTM -> Praxis AI edge -> Grid-selected Praxis provider gateway
```

The production deployment inventory contains:

| Layer | Operational inventory |
|---|---|
| GTM | Public service name, public TLS ownership, edge origins, health probes, steering policy, drain/failback policy, DDoS/WAF controls. |
| Edge | At least two failure domains, pinned Grid/AI/Praxis compatibility set, caller authentication, tenant/model authorization, request limits, accepted overlay status. |
| Grid | One edge-specific `GatewayRef` and routing perspective per edge location, authenticated site state, bounded admission policy, versioned overlay revisions. |
| Provider | Private backend, provider Praxis gateway, mTLS listener, trusted edge identities, local authorization, local limits, and final-hop credentials. |

### Edge Readiness

A production GTM integration uses a route-aware readiness signal. The edge is
ready when:

- its public listener and external authentication dependencies are healthy;
- a supported overlay version has been accepted;
- the accepted overlay age is within policy;
- endpoint/TLS configuration exists for every usable selected cluster; and
- the public offer has its configured minimum authorized route coverage.

One unavailable optional provider does not withdraw the edge. Loss of all
required routes, a hard-expired snapshot, or a failed security dependency does.
Liveness remains a process/listener signal and is not used as route coverage.

During drain, readiness is withdrawn before shutdown. New requests stop while
existing SSE streams receive the configured completion interval.

### Overlay Rollout Evidence

A production external-edge rollout tracks four distinct states:

```text
desired revision
  -> rendered ConfigMap revision
  -> distributed file revision
  -> Praxis accepted/serving revision
```

`GridNetwork.status.consumerConfigStatus=Rendered` reports successful desired
config rendering and apply. It does not report that the gateway loaded the
overlay. The production contract requires gateway status for the accepted
revision, digest, acceptance time, age, and last rejection reason. That
contract is not satisfied by compatibility profiles that expose only
`Rendered` status.

Operational checks compare the same revision across all four states and send a
request whose internal route-decision record contains that revision.

An invalid overlay is expected to:

1. fail strict parse or semantic validation;
2. leave the previous accepted snapshot serving;
3. increment a bounded rejection metric;
4. expose the rejection reason without content or secrets; and
5. affect readiness only according to accepted-snapshot age policy.

### Provider Trust Verification

Control-plane `GridSite Active` is necessary but not sufficient. The data-plane
verification uses the actual edge-to-provider path:

```text
known edge certificate -> accepted
unknown/revoked certificate -> rejected
wrong SNI/server identity -> rejected
direct public client -> rejected
authorized peer but denied provider policy -> rejected
```

The provider gateway's backend remains `ClusterIP`-only or otherwise private.
Customer `Authorization` values are absent from the provider request. Provider
credentials exist only at the final hop.

## Development Validation Environments

The `xtask env` commands provide a local development and
integration-validation path using Kind clusters. They are not the production
reconciliation model. The Kubernetes-native global-ingress scenario is
documented in
the [Praxis demos repository](https://github.com/praxis-proxy/demos).

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
| `cargo xtask env deploy-consumer-gateway` | Deploys a consumer Praxis AI gateway with a generated static `intelligent_route` config |
| `cargo xtask env deploy-consumer-gateway --overlay-config <path>` | Deploys the consumer gateway using a `routing-config.json` routing overlay file |
| `cargo xtask env verify-gateway-e2e` | Verifies consumer-to-provider routing end-to-end |
| `cargo xtask env verify-mtls-trust` | Verifies provider gateway mTLS enforcement (positive + negative cases) |
| `cargo xtask env verify-api-fallback-native` | Verifies native `intelligent_route` → `credential_inject` credential injection with token bytes absent from overlay and consumer ConfigMap |
| `cargo xtask env verify-stale-gc-ttl` | Verifies `GridNetwork.spec.staleCandidateTtlSeconds` evicts stale remote candidates from the rendered overlay |
| `cargo xtask env verify-responses-routing` | Verifies `/v1/responses` request parsing and Grid overlay routing using `openai_responses_format` → `intelligent_route` filter chain |
| `cargo xtask env verify-crd-schema` | Verifies required generated CRD schema fields without requiring kind clusters |
| `cargo xtask env verify-operator-install-rbac` | Applies install manifests, runs positive/negative RBAC checks, proves minimal reconcile succeeds |
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

#### Three-node SWIM mesh

```console
cargo xtask env verify-swim-mesh-three-node -c tests/env/operator-routing-multisite.toml
```

This command starts three SWIM-enabled operator processes in a linear topology:
node A (no seeds), node B (seeds A), and node C (seeds B only — not A).  It proves:

1. **Transitive discovery** — A learns about C through B.  After gossip convergence,
   `GridNetwork.status.distributedProviderCount >= 2` on A, confirming CRDT state from
   both B and C reached A transitively.

2. **Routing eligibility before Active** — C's CRDT provider is present in A's
   SWIM state but absent from A's routing overlay because C's `GridSite` is not yet
   `Active`.  Both B and C are excluded.

3. **Routing eligibility after Active** — After C's `GridSite` is set to `Active`
   (with a reachable egress address), A's overlay is re-rendered and C's provider
   candidate appears.

4. **Cross-network isolation** — A wrong-network `GridNetwork` and `InferenceProvider`
   are applied alongside the main network.  The wrong-network model is confirmed absent
   from A's correct-network overlay, proving providers cannot cross network scopes.

This validation proves that SWIM gossip alone is not sufficient for routing; explicit
`Active` phase assignment is required.

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

This table applies to the generic `xtask env` harness above
(`validate-all`, `verify-swim-mesh-three-node`,
`verify-failover-under-lost-peer`, etc.), which is the only
path that consumes these two locally-built defaults directly.
The named demos (`grid-glb-demo`, `grid-combined-site`,
`grid-llmd-pool-metrics`) do **not** need them — they override
`GRID_XTASK_GATEWAY_IMAGE`/`GRID_XTASK_MOCK_EPP_IMAGE`
(see `xtask/src/env/image_overrides.rs`) with published
`ghcr.io/praxis-proxy/grid-ai-rollup` images and never build
from an AI repository checkout.

As of this writing, neither `Containerfile.composed` nor a mock
llm-d Endpoint Picker server implementation exists in the AI
repository (tracked in
[`ai#716`](https://github.com/praxis-proxy/ai/issues/716)), so
`build-gateway-images --ai-repo <path>` cannot currently produce
either of the first two images. That gap blocks only the
generic-harness path — concretely, `verify-failover-under-lost-peer`
today — not any of the three named demos above. It is itself
soft-blocked on the in-flight, other-team-owned
[`ai#334`](https://github.com/praxis-proxy/ai/pull/334)
(`ext_proc` compatibility moving into the AI repository), which is
deliberately paused pending a release-timeline decision rather than
abandoned.

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
- They do not perform live config reload against
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
accepts a `routing-config.json` routing overlay file. This
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

When an overlay is supplied, `intelligent_route.local_site`
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
in the accompanying research-spikes repository.

Grid keeps generic, config-driven, reusable commands.
Topology-specific fixtures, static manifests, and
presentation walkthroughs belong outside the Grid
repository.

## References

- [HashiCorp memberlist](https://github.com/hashicorp/memberlist) — reference
  design for SWIM-style membership, gossip transport encryption, key rotation,
  and join/admission behavior. Grid uses foca rather than memberlist; foca used
  the Go-based memberlist implementation as a reference architecture, and Grid
  uses memberlist the same way: as an architectural reference for control-plane
  gossip hardening.
