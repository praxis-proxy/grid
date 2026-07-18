# Grid Operations Walkthrough

Step-by-step guide to grid formation, site lifecycle,
and routing configuration.

## 1. Deploy the Grid Operator

### Install

Generate and install CRDs, then apply the operator install
package:

```console
cargo run -p operator --bin generate_crds | kubectl apply -f -
kubectl apply -f deploy/operator/
```

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

The operator image must be built and loaded before the
Deployment starts.  For Kind clusters, build the binary
with `cargo build -p operator --bin operator`, then use
`deploy/operator/Containerfile` to build the image and
`kind load docker-image` to load it.  For production,
replace the `image:` field in the Deployment with your
registry path.

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

**Core resources (namespaced, `grid-operator-resources`):**

| Resource | Verbs | Why |
|---|---|---|
| `secrets` | `get`, `create`, `patch` | Read TLS certs, SWIM key, credential refs; SSA-create CA and site cert `Secrets` |
| `configmaps` | `create`, `patch` | SSA-create routing overlay and consumer config `ConfigMaps` |

The `grid-operator-resources` `ClusterRole` is never bound
cluster-wide.  It takes effect only in namespaces where a
`RoleBinding` references it.

### Secret access boundaries

The operator reads `Secrets` in the namespace declared by
each `SecretRef` in the CRD spec.  It does not search
across namespaces or list `Secrets`.

| Secret path | Keys read | Keys written |
|---|---|---|
| `spec.tls.siteSecretRef` | `tls.crt` only (`tls.key` is never read) | `tls.crt`, `tls.key` (create-if-absent via SSA patch) |
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
| Routing overlay | `grid-overlay-{network}-{gateway}` | `grid-config.json` | `GatewayRef.namespace` |
| Consumer config | `consumerConfig.configMapName` | `praxis.yaml` | `GatewayRef.namespace` |

### What is not granted

Neither `ClusterRole` grants:

- `events` — the operator does not emit Kubernetes Events
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
| `GRID_GATEWAY_ADDRESS` | Gateway address for site discovery |

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
namespace-scoped boundary proofs), then waits for the
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
`GridSite` is `Active`.  Both layers are required for production deployments.

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
- Wait for the SWIM probe failure window (default: ~10 s `suspicionTimeout`).
  The peer is declared `Dead` and `GridNetwork.status.connectedSites` decreases.
- If the remote operator is still running, it will rejoin as `Alive` again because
  SWIM membership is peer-to-peer — seeds are only needed for initial discovery.

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

2. **Gateway address known** — the remote operator advertises `GRID_GATEWAY_ADDRESS`
   via SWIM state broadcast.  The local operator stores it in `GridSite.spec.egress.address`.
   Phase: `Connecting`.  No trust established.

3. **Public cert material received** — the remote operator broadcasts its public site
   certificate PEM.  The operator validates the PEM structure (rejects private-key markers;
   checks for `CERTIFICATE` header) and stores it in `GridSite.status.publicCertPem`.
   Reason: `TrustPolicyMissing` (cert received but no fingerprint policy configured).

4. **TCP gateway probe passes** — the `GridSite` controller can reach the gateway
   address.  Phase stays `Connecting` until a trust policy is configured.

5. **Trust policy verified** — configure `spec.trust.certFingerprint` with the SHA-256
   fingerprint of the remote site's `publicCertPem`.  When the fingerprint matches and
   the TCP probe succeeds, the operator promotes the site to `Active` with reason
   `TrustPolicyVerified`.

   ```bash
   # Read the received certificate fingerprint
   FP=$(kubectl get gridsite <name> -o jsonpath='{.status.publicCertPem}' | \
        sha256sum | awk '{print $1}' | sed 's/\(..\)/\1:/g;s/:$//')
   # Configure the fingerprint trust policy
   kubectl patch gridsite <name> --type=merge \
     -p '{"spec":{"trust":{"certFingerprint":"'"$FP"'"}}}'
   ```

   See [Authentication and Access Policy](auth.md) for the trust contract.

6. **Data-plane mTLS enforced** — the provider gateway enforces peer identity via mTLS
   on every request, independent of the control-plane phase.

### Authentication vs authorization

| Concept | Question it answers | Grid mechanism |
|---|---|---|
| Authentication | "Is this peer really the site it claims to be?" | Gateway mTLS peer identity and certificate validation |
| Authorization | "Is this authenticated peer allowed to participate in this Grid or receive/send this traffic?" | Grid trust policy, allowed peer identity, and gateway enforcement |

A peer must satisfy both.  A SWIM peer must never become routable solely because
it gossiped successfully.

### Security boundary

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

`GridSite.status.phase == Active` is the control-plane gate for remote CRDT provider records.
Provider records advertised by a SWIM peer are included in the routing overlay only when
the corresponding `GridSite` is `Active`.  Peers in `Discovered`, `Connecting`, or any
other phase are excluded.  Peers with no matching `GridSite` are also excluded (fail-closed).

Setting `Active` requires an explicit trust policy.  For the current operator, that
policy is `GridSite.spec.trust.certFingerprint`: when the configured fingerprint
matches the received public certificate and the TCP probe succeeds, the operator
promotes the site to `Active`.  Data-plane mTLS at the provider gateway enforces
peer identity on every request independently of the control-plane phase.

## 5. Connectivity Verification

The current `GridSite` controller verifies gateway reachability with a TCP probe
against `spec.egress.address`.

| Condition | Current check |
|-----------|---------------|
| `SWIMReachable` | SWIM membership reports the peer Alive |
| `GatewayAddressKnown` | `spec.egress.address` is non-empty |
| `GatewayReachable` | TCP connect to `spec.egress.address` succeeds |

mTLS trust verification and request-time authorization are enforced by the
gateway data plane.  Advancing a site to `Active` requires the deployment
workflow to establish trust and data-plane readiness.

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

A non-empty value means the remote operator is advertising its site certificate.
A `Connecting` site with a non-empty `publicCertPem` and a reachable gateway
(TCP probe succeeded) is ready for fingerprint trust verification.  Configure
`spec.trust.certFingerprint` to advance it to `Active`.

A site in `TrustMaterialMissing` has a reachable gateway but no certificate.
Configure `spec.tls.siteSecretRef` on the remote `GridNetwork` to enable certificate advertisement.

### Security boundary

The public certificate recorded in `status.publicCertPem` is **not** automatically
trusted.  The control plane records received trust material for operator visibility.
The provider gateway enforces mTLS peer identity and certificate validation on every
request — the control plane record does not bypass that check.

Private keys are never included in SWIM broadcasts.  The operator reads only the
public certificate (`tls.crt`) from the site Secret, not the private key (`tls.key`).

## GridSite gateway address configuration

Set `GRID_GATEWAY_ADDRESS` on the operator process to advertise the data-plane
gateway endpoint to SWIM peers.  This address is propagated through SWIM state
broadcasts and used by the primary operator to populate `GridSite.spec.egress.address`
for auto-discovered sites.

```bash
# Example: operator running alongside a Praxis gateway on port 8080
GRID_GATEWAY_ADDRESS=10.0.0.4:8080 ./operator
```

**Requirements:**
- Format: `host:port` or `IP:port` (any non-empty string is accepted; the remote
  operator stores it verbatim in `GridSite.spec.egress.address`)
- When absent or empty: auto-discovered `GridSite` records have empty
  `spec.egress.address` and stay in `Discovered` phase
- This address is separate from `GRID_SWIM_BIND_ADDR` — the SWIM gossip endpoint
  and the data-plane gateway address are distinct

**Probe behavior:** The `GridSite` controller probes `spec.egress.address` with a
TCP connect (5-second timeout) on each reconcile.  A successful probe reports
`reason: GatewayReachable`.  A failed probe reports `reason: GatewayUnreachable`.
An Active site is demoted to Unreachable when its probe fails.

**Trust behavior:** The TCP probe is not an authentication or authorization
check.  `GatewayReachable` means the address is reachable; it does not prove the
remote peer identity.  Use gateway mTLS peer identity enforcement and Grid trust
policy before treating a site as authorized for traffic.

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
| Connecting | Active | `GridSite` controller: TCP probe succeeds and `spec.trust.certFingerprint` matches `status.publicCertPem` |

Security invariant: a SWIM peer must never become routable solely because it
gossiped successfully.  Discovery, authentication, and authorization are
separate steps.

### Troubleshooting

**Phase stays Pending after SWIM convergence**

- Check the `GridNetwork` has the label `grid.praxis-proxy.io/auto-discover-sites: "true"`.
- Check that the `GridNetwork` controller has SWIM running (`GRID_SWIM_BIND_ADDR` env var set).
- Check `kubectl get gridnetwork <name> -o jsonpath='{.status.connectedSites}'` — must be > 0.

**Phase stays Discovered (not advancing to Connecting)**

- The site has no `spec.egress.address`.  Configure `GRID_GATEWAY_ADDRESS` on the remote
  operator and wait for the next reconcile to propagate the gateway address through SWIM.
- Reason will be `GatewayAddressMissing`.

**Phase stays Connecting**

- Check `status.reason`:
  - `TrustPolicyMissing`: the TCP probe succeeded and public certificate material was received, but
    `spec.trust.certFingerprint` is not configured.
  - `TrustPolicyMismatch`: the TCP probe succeeded and public certificate material was received, but
    the configured fingerprint does not match the received certificate.
  - `TrustMaterialMissing`: the TCP probe succeeded, but no public certificate has been received.
  - `TrustMaterialInvalid`: received trust material failed the structural PEM check.
  - `GatewayUnreachable`: the TCP probe to `spec.egress.address` failed.  Verify the gateway
    is running and `GRID_GATEWAY_ADDRESS` is correct on the remote operator.
  - `GatewayAddressMissing`: no egress address is set.  Configure `GRID_GATEWAY_ADDRESS` on
    the remote operator.

**Phase is Active, site became Unreachable**

- The TCP probe against `spec.egress.address` failed.  The `GridSite` controller moved the
  site from Active to Unreachable.  When the gateway becomes reachable again, the site moves
  to Connecting.  Returning to Active requires the gateway to be reachable and
  `spec.trust.certFingerprint` to match the received public certificate.

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

### Credential Secret access

The generated `ConfigMap` references credential Secrets by name, namespace, and
key — it does not read Secret values.  The operator does NOT require `get` access
to credential Secrets in the gateway namespace for config generation.

The consumer gateway pod needs the credential Secret mounted.  Secret provisioning
in the consumer cluster is the responsibility of external tooling (platform
automation, External Secrets, Vault, or a manual process).  The Grid operator does
not copy Secrets across clusters.

### Cross-cluster limitations

The operator's RBAC controls access within its own cluster.  When the consumer
gateway runs in a different cluster, the generated `ConfigMap` must be delivered
externally — the operator cannot write to a remote cluster's API server directly.
The Kind validation harness (`verify-api-fallback-native`) bridges this gap for
local testing by reading the generated YAML and re-applying it as
`praxis-consumer-config` in the consumer cluster.  Production cross-cluster
delivery requires GitOps, External Secrets, or a similar mechanism.

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
5. SWIM propagates public certificate material; the operator
   verifies the fingerprint and advances matching sites to `Active`
6. Once `Active`, the new site's providers are visible
   to all other sites through the routing overlay

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
   from A's correct-network overlay, proving providers cannot cross network boundaries.

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
in the accompanying research-spikes repository.

Grid keeps generic, config-driven, reusable commands.
Topology-specific fixtures, static manifests, and
presentation walkthroughs belong outside the Grid
repository.

## References

- [HashiCorp memberlist](https://github.com/hashicorp/memberlist) — reference
  design for SWIM-style membership, gossip transport encryption, key rotation,
  and join/admission behavior. Grid uses foca rather than memberlist; the
  memberlist model is used only as an architectural reference for control-plane
  gossip hardening.
