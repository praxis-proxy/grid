# Operator-Generated Consumer Config

The AGN Operator can generate the consumer Praxis `ConfigMap` from routing overlay
data.  This is an opt-in feature on each `GatewayRef`.

## Migration: `clusterEndpoints` transport shape change

The `clusterEndpoints[]` field shape has changed.  The bare `sni` field has been
replaced by an explicit `transport` block.  Existing configs must be updated.

**Before (no longer accepted):**

```yaml
clusterEndpoints:
  - cluster: site-a
    address: "10.0.0.4:30080"
    sni: site-a.grid.internal
  - cluster: api-provider
    address: "mock-api.default.svc:8080"
```

**After (required):**

```yaml
clusterEndpoints:
  - cluster: site-a
    address: "10.0.0.4:30080"
    transport:
      mode: mutual_tls
      sni: site-a.grid.internal
  - cluster: api-provider
    address: "mock-api.default.svc:8080"
    transport:
      mode: plaintext
```

Key differences:

- `sni` moves from a top-level field to `transport.sni`.
- `transport.mode` is the security switch (`mutual_tls`, server-authenticated
  `tls`, or explicit insecure/dev-only `plaintext`), not `sni` presence.
- Missing `transport` fails closed — the operator will not render the cluster entry.
- `plaintext` must not set a nonblank `sni` (rejected as likely misconfiguration).

### Custom backend CA Secret namespace

`clusterEndpoints[].transport.caSecretRef` is namespace-local to the target
gateway. Specify the Secret `name` and, optionally, its `key`; the Secret must
exist in the namespace from that entry's `GatewayRef.namespace`:

```yaml
transport:
  mode: tls
  sni: api.example.internal
  caSecretRef:
    name: api-provider-ca
    # key: ca.crt
```

This is a field-specific API change: remove `namespace` from existing
`transport.caSecretRef` values and ensure the Secret is in the target gateway
namespace before applying the updated `GridNetwork`. Kubernetes prunes unknown
fields from CRD requests: `Warn` mode accepts the object and reports a warning,
while `Ignore` mode drops the field silently. `Strict` mode rejects the request;
`kubectl --validate=true` uses strict validation where supported. Existing
objects accepted under the previous schema should be updated from a manifest
with the field removed. Do not rely on an old namespace value or move/copy
Secrets across namespaces. This change does not affect
`GridNetwork.spec.tls.caSecretRef`, provider credential references, or provider
health-check TLS references; those keep their existing explicit namespace
behavior.

## Implemented: GatewayRef.consumerConfig

When `spec.gatewayRefs[].consumerConfig.enabled: true`, the `GridNetwork`
controller renders a `praxis.yaml`-keyed `ConfigMap` in the gateway namespace
when consumer configuration renders successfully. An empty candidate overlay
produces a fail-closed 503-only config without route filters. For nonempty
candidates, the generated config includes:

**Validation status:** `verify-api-fallback-native` proves end-to-end runtime
consumption of the operator-generated `ConfigMap`.  The xtask harness reads the
exact `praxis.yaml` from `op-e2e-consumer-config` in the provider cluster,
applies it byte-for-byte as `praxis-consumer-config` in the consumer cluster, and
confirms all 9 routing assertions pass with the live consumer pod running the
operator-generated config.  Token bytes are absent from the `ConfigMap`,
consumer-cluster replica, overlay JSON, and all logs.

The generated config is a complete, runnable Praxis config containing:

- `listeners:` — one public listener at `0.0.0.0:{listenerPort}` (default 8080)
- `filter_chains:` — the consumer filter chain:
  - `intelligent_route` candidates from the overlay (with `credential.secretRef` for
    credential-bearing candidates)
  - `credential_inject` entries using `file:` sources when credential-bearing
    candidates are present — token bytes are never written to the `ConfigMap`
  - `load_balancer` entries (one per unique candidate cluster). Every referenced
    cluster must have a matching `consumerConfig.clusterEndpoints[]` entry with
    endpoint address and explicit `transport` configuration (`mutual_tls`,
    `tls`, or `plaintext`). Missing transport fails closed: the operator will
    not silently render a plain-HTTP cluster when transport intent is absent.
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

Set `consumerConfig.telemetry` to add process-level OTLP settings and the
`trace_context` propagation filter. These settings are written at the Praxis
config root and never enter the routing overlay. The consumer ConfigMap does
not contain collector headers; configure `OTEL_EXPORTER_OTLP_HEADERS` on the
gateway Deployment with a Secret-backed environment reference. See
[OpenTelemetry for Grid gateways](opentelemetry.md) for examples and the
Praxis 0.7.1 trace-linkage limitation.

This generated config covers the direct API-provider path where the consumer
gateway is also the final-hop gateway for the provider API call. A credential
reference is rendered and mounted only on the gateway whose local site matches
the candidate's site. Remote candidates therefore receive their credentials at
the provider site's final backend hop, rather than at an earlier gateway.

The generated config requires a Praxis AI image that contains the
`credential_inject` filter. AGN can render the config and project
credential references today; deployments must ensure the selected Praxis AI image
includes the matching request-time filter.

See [`docs/architecture/crds.md`](crds.md#gatewayrefconsumerconfig) for the full
field reference.

## Derived endpoint topology

`consumerConfig.clusterEndpoints[]` is normally supplied by whoever manages the
gateway deployment. With `consumerConfig.deriveTopology.fromProviders`, the
operator works out an entry for every candidate cluster that has none, from the
declarations of the providers that list names.

The allowlist is the trust decision. An `InferenceProvider` is cluster scoped
and its `gridNetworkRef` is self asserted, so registering one must not by itself
decide where a gateway dials or what it trusts. Naming a provider in
`fromProviders` is the gateway owner accepting that provider's declarations. An
empty list derives nothing, because that is the state a half-finished edit
leaves behind and it has to mean no provider rather than every provider.

Derivation emits the same entries the field holds, so nothing downstream
changes: every validation and reason code below applies to a derived entry
exactly as it does to a supplied one. The opt-in is per gateway, so one site can
adopt derivation while another keeps its explicit entries.

**A candidate at this gateway's own site** derives from its provider's
`spec.endpoint`:

| Field | Source |
|---|---|
| `address` | endpoint host and port, port defaulted by scheme (443, or 80 for `http`) |
| `transport.mode` | `tls` for an `https` endpoint, `plaintext` for `http` |
| `transport.sni` | `spec.backendTls.serverName`, else the endpoint host |
| `transport.caSecretRef` | `spec.backendTls.caSecretRef`, named in the gateway namespace, else absent |

The URL scheme chooses the transport, so `backendTls` declares no mode and
cannot contradict the endpoint. An absent `caSecretRef` means the process trust
store, as it does for a supplied entry, so a privately signed backend declares
one.

**A candidate at another site** derives from that site's `GridSite`:

| Field | Source |
|---|---|
| `address` | `spec.egress.address` |
| `transport.mode` | `mutual_tls` for `Mutual`, `plaintext` for `Plaintext` |
| `transport.sni` | `spec.egress.tls.serverName` |

Client identity for the hop is the grid identity the gateway already mounts at
`tlsCertMountPath`, so no backend credential crosses a site boundary. A site
declaring `Mutual` without a `serverName` derives an entry with no SNI, which
fails closed as `MissingSni` rather than verifying a name nobody declared.

**What it will not do.** An explicit entry wins whole, never field by field, so
a partially filled entry keeps its own gaps and its own reason code instead of
inheriting derived trust. A provider outside `fromProviders`, in another
network, or explicitly `Unavailable` is not a source. Two providers claiming one
routing identity, or one cluster appearing at two sites, derive nothing rather
than letting iteration or name order pick. A remote hop derives only from a site
in phase `Active`, since only that site has had its address probed and its leaf
pinned; a `Discovered` or `Connecting` stub carries an address copied from
gossip. An endpoint whose port the URL declares but cannot represent, such as
`:99999`, refuses rather than falling back to the scheme default. An `https`
endpoint named by address needs `backendTls.serverName`, because Praxis rejects
an IP literal as an SNI and deriving one would emit a config the gateway
refuses to load. Trust is declared and never inferred, and
verification is never relaxed to make a connection succeed. A candidate at
another site reaches that site's provider hop whatever its URL looks like: a
publicly resolvable model URL is not permission for direct access. A cluster with nothing to derive from is left out. The renderer then reports
`MissingClusterEndpoint`, and because it collects its cluster entries into one
result, the first unresolved cluster fails the whole consumer-config render and
the gateway keeps its previous configuration for every tenant. That is
fail-closed rather than per-cluster, and it is the same behaviour an unsupplied
explicit entry has always had.

**Endpoint base paths.** Not carried. Praxis has no per-cluster upstream base
path to render one into, so an endpoint URL's path is read by nothing today and
is not resolved. Tracked by
[issue 248](https://github.com/praxis-proxy/grid/issues/248).

The gateway's `consumerConfigStatus` message names the clusters whose entries
were derived, so a reader can tell where a value came from without reading the
generated config.

## Operational diagnostics

After enabling `consumerConfig.enabled: true` for a gateway, the `GridNetwork`
status reports the outcome under `status.consumerConfigStatus[]`.

### Delegated mount reconciliation

Secret mount management remains opt-in. Set
`consumerConfig.mountReconciliation.enabled: true` and name the exact Deployment
and Praxis container. The operator verifies that the Deployment carries the
matching explicit opt-in annotations and mounts the generated `praxis.yaml`
ConfigMap at `/etc/praxis` before it patches volumes or mounts.
`charts/praxis-gateway` can add those annotations with
`mountReconciliation.enabled`; its `mountReconciliation.network` and
`mountReconciliation.gatewayRef` must match the GridNetwork and `GatewayRef`.

Point the chart at the operator-generated config map with
`config.existingConfigMap: praxis-consumer-config` and keep
`gatewayConfig.render: false`. List the credential Secret names in
`mountReconciliation.managedCredentialNames`, and keep
`mountReconciliation.releaseHelmMounts: false` for the preparation phase. The
chart retains all credential and TLS mounts during this phase. For an existing
release, move `consumerConfig.credentialMountBase` and (when consumer mTLS is
used without Grid serving) `consumerConfig.tlsCertMountPath` to paths that do
not overlap Helm mounts. Grid installs its mounts and rolls the generated
config while the prior Helm files remain available. After
`mountReconciliationStatus: Ready`, set `releaseHelmMounts: true` and upgrade
the chart to remove only the selected old credential mounts and, without Grid
serving, the old TLS projection. Listener TLS and other chart-managed Secret
mounts remain Helm-owned.

With `gridServing.enabled`, the chart retains its TLS projection at
`/etc/praxis/tls` permanently because the serving pollers read those files.
The chart adds a marker that lets Grid validate those Secret keys and include
their resource versions in rollout decisions without taking ownership of the
mount. The chart's `tls.existingSecret` and `tls.caSecret` must match the
GridNetwork's site identity and CA references; the chart enforces the fixed
mount path.

Create the referenced ConfigMap with a valid bootstrap `praxis.yaml` before
installing the gateway. Once the bootstrap Deployment is ready, Grid writes the
generated config to an inactive, Grid-managed ConfigMap slot and changes the
Pod template's config source, Secret projections, and revision annotations in
one patch. Old pods keep their previous config and projections during rollout.
Grid waits until no old replicas remain before reusing the inactive slot or
pruning obsolete mounts. This also lets a new Deployment become ready before
Grid replaces the bootstrap config. This verifies the Kubernetes rollout, not
Praxis acceptance of populated routes. The current generated inline candidates
include `admission_state` and `selection_group`, which the tested Praxis image
rejects. Populated-route use requires the versioned-overlay config in
[Grid #270](https://github.com/praxis-proxy/grid/pull/270), a compatible image
containing [Praxis AI #1539](https://github.com/praxis-proxy/ai/pull/1539), and
an unmodified generated-config request probe. Until then, do not release
Helm-owned mounts for a populated route based on Deployment or mount `Ready`
status alone.

The operator publishes a reference-only ConfigMap named
`grid-mount-requirements-<first 16 hex characters of SHA-256(configMapName)>`,
with its document under `mount-requirements.json`. It verifies every
referenced Secret and required key in the gateway namespace, and adds only its
reserved volumes and the selected container's mounts. It stages the matching
Praxis configuration and Secret mounts in the same Pod revision, including for
Secret reference and key changes. When a reference is removed, the old mount
remains until pods with the new config are ready, then Grid removes only mounts
recorded as Grid-owned.
If the last provider disappears, the operator distributes an empty authoritative
routing overlay and renders the generated consumer config as a 503-only response
with no `intelligent_route` filter. For delegated mounts, the empty overlay is
published before the 503-only config is reconciled, so consumers using the
watched overlay stop selecting providers even if config reconciliation fails.
A static-only consumer still needs the 503-only config to apply and reload.
Obsolete Grid-owned mounts are pruned only after the matching config rollout is
ready. Other Deployment fields, containers, volumes, mounts, and Helm resources
are preserved.

For delegated mounts, all credential, Grid CA, site identity, and custom backend
CA Secrets must be in the gateway namespace. For server-authenticated `tls`
endpoints, `clusterEndpoints[].transport.caSecretRef` selects a CA Secret from
that gateway namespace; its key defaults to `ca.crt`. Mutual TLS uses the Grid
CA and site identity from `GridNetwork.spec.tls`. Secret contents and private
keys are never copied into generated ConfigMaps, status, or logs.
The operator does not maintain a per-reference Secret allowlist: an
`InferenceProvider` author can select a Secret key in the gateway namespace for
the final-hop credential. Restrict `InferenceProvider` writes to trusted
control-plane users and keep only gateway-authorized credentials in that
namespace. The chart's `managedCredentialNames` supports mount handoff; it is
not an authorization list.
When mount reconciliation is disabled, the operator still renders the mTLS
file paths but leaves their mounts with the gateway owner; Grid CA and site
identity Secret references are required only for delegated mounts.
Disabling mount reconciliation or deleting the `GridNetwork` does not remove
previously Grid-owned Deployment mounts. Hand ownership back to the Deployment
manager or remove those mounts explicitly after moving the gateway off the
generated configuration. Before disabling delegation, move the Deployment's
config volume back to `consumerConfig.configMapName` and wait for its rollout;
the alternate slot is only maintained while delegation is enabled. Do not
treat disabling the feature as credential revocation; revoke or rotate the
Secret and verify the Deployment separately.

`consumerConfigStatus[].phase: Rendered` means the config map was rendered and
applied. It does not mean the gateway has restarted or become ready. With mount
reconciliation enabled, `mountReconciliationStatus[]` reports
`MountsReconciling`, `WaitingForSecret`, `WaitingForRollout`, `Ready`, or
`Error`. `Ready` requires a completed Deployment rollout with no old replicas;
the selected Praxis container must mount Grid's generated ConfigMap and
`praxis.yaml` key at `/etc/praxis`. Secret resource versions are hashed before
they are placed in pod annotations; Secret values are never used as rollout
metadata.
A deliberately scaled-to-zero Deployment stays `WaitingForRollout` until pods
are started and a complete rollout proves the mounts and config are present.

The operator's `grid-operator-resources` RoleBinding needs `deployments` `get`
and `patch` in the gateway namespace for this opt-in feature. With a nonempty
candidate overlay and the feature disabled, the operator publishes the
requirements document but does not read or patch gateway Deployments.

### Reading consumer config status

```console
kubectl get gridnetwork production -o jsonpath='{.status.consumerConfigStatus}' | jq .
```

Example success output:

```json
[
  {
    "gatewayName": "inference-gw",
    "namespace": "praxis-system",
    "configMapName": "praxis-consumer-config",
    "phase": "Rendered",
    "reason": "",
    "message": "consumer config rendered and applied to praxis-system/praxis-consumer-config",
    "observedGeneration": 7
  }
]
```

Example failure output:

```json
[
  {
    "gatewayName": "inference-gw",
    "namespace": "praxis-system",
    "configMapName": "praxis-consumer-config",
    "phase": "Error",
    "reason": "ConsumerConfigApplyFailed",
    "message": "kube error: ...",
    "observedGeneration": 7
  }
]
```

### Reason codes

| Reason | Phase | Meaning |
|---|---|---|
| _(empty)_ | `Rendered` | Config rendered and `ConfigMap` applied successfully |
| `MissingClusterEndpoint` | `Error` | A candidate cluster is missing from `consumerConfig.clusterEndpoints[]` |
| `MissingTransport` | `Error` | A cluster endpoint has no `transport` configuration — the operator refuses to guess TLS vs plaintext |
| `MissingSni` | `Error` | A `mutual_tls` or `tls` cluster endpoint has no (or blank) `sni`; TLS requires a server name |
| `PlaintextWithSni` | `Error` | A `plaintext` cluster endpoint has `sni` set — `sni` does not enable TLS; use `mutual_tls` if TLS is intended |
| `ConsumerConfigRenderFailed` | `Error` | Overlay data produced an unrenderable config (e.g. blank local site) |
| `ConsumerConfigApplyFailed` | `Error` | Kubernetes API rejected the `ConfigMap` apply (e.g. RBAC, namespace not found) |
| `ConsumerConfigError` | `Error` | Other error during render or apply |

### Troubleshooting

**Phase is `Error` / reason `ConsumerConfigApplyFailed`**

The operator could not apply the `ConfigMap`.  Common causes:

- Missing RBAC: the operator's `ServiceAccount` lacks `configmaps` `create`
  and `patch` in the gateway namespace.  See the
  [RBAC permissions](operations.md#rbac-permissions) in the operations guide.
- The namespace does not exist.  Create it before enabling `consumerConfig`.
- Kubernetes API server is temporarily unavailable.  The reconcile will retry on
  the next requeue (default 5 minutes) or when the `GridNetwork` or any watched
  `InferenceProvider` changes.

**Phase is `Error` / reason `ConsumerConfigRenderFailed`**

The overlay data produced a structural error.  Check that `localSiteName` is set
on the `GatewayRef` (or that the `GridNetwork` name is a valid site identity) and
that all provider `routingClusterRef` values are non-empty.

**Phase is `Error` / reason `MissingClusterEndpoint`**

At least one route candidate references a cluster with no corresponding
`consumerConfig.clusterEndpoints[]` entry.  Add an endpoint entry for the reported
cluster before restarting or rolling out the consumer gateway.

**Phase is `Error` / reason `MissingTransport`**

A cluster endpoint has no `transport` field.  The operator requires every
`clusterEndpoints[]` entry to declare explicit transport intent — either
`mutual_tls` or `tls` (both with `sni`), or `plaintext`. Add a `transport` block to the
identified endpoint.  The operator will not guess whether a cluster should use
TLS or plaintext.

**Phase is `Error` / reason `MissingSni`**

A `mutual_tls` or `tls` cluster endpoint has a blank or missing `sni` field. The `sni`
must match the Subject Alternative Name in the provider gateway's server
certificate.  Add a non-blank `sni` to the endpoint's `transport` block.

**Phase is `Error` / reason `PlaintextWithSni`**

A `plaintext` cluster endpoint has `sni` set.  Setting `sni` on a plaintext
transport does not enable TLS — it is almost certainly a misconfiguration.
Either change the mode to `mutual_tls` (if TLS is intended) or remove `sni`
from the endpoint.

**Consumer pod has not applied an updated Praxis ConfigMap**

The operator updates the ConfigMap; it does not restart gateway pods. A Praxis
build with file watching reloads supported routes, filter pipelines, and
load-balancer endpoints after the kubelet refreshes the mounted file. Mount the
directory rather than a `subPath`, and check gateway logs for acceptance.
Listener and other startup settings still require a restart. The routing
overlay is a separate file that `intelligent_route` validates and reloads. See
[Reload and rollout](#reload-and-rollout) below.

When no inference candidates remain, the operator distributes an empty
authoritative routing overlay and updates its generated consumer `ConfigMap` to
a 503-only response with no `intelligent_route` filter. The ConfigMap update
does not itself restart gateway pods; use a supported file reload or restart
when the running gateway does not reload it. The empty overlay also fails closed
for routes already using the dynamic overlay.

## Edge-ingress deployments

External edge-ingress gateways reuse the same consumer config contract: the
operator renders a `ConfigMap` with static endpoint topology and `intelligent_route`
candidates, and the edge gateway consumes it the same way a cluster-local
consumer gateway does.

The key distinction for edge deployments is that the routing overlay data
(candidate membership, ordering, freshness) changes more frequently than
static endpoint/TLS topology.  The intended architecture separates these:

- **Praxis topology** (`praxis.yaml` listeners, endpoints, and filter chains):
  supported pipeline and endpoint changes use file reload; listener and other
  startup settings require a restart. Certificate file reload depends on the
  gateway image and the component reading the files.
- **Dynamic overlay** (`routing-overlay.json` envelope): changes are consumable
  without a full restart through `intelligent_route` overlay-file hot reload.

Praxis AI validates each projected envelope before atomically replacing the
in-memory route snapshot. A malformed replacement retains the same-process
last-known-good snapshot. The deployment must mount the projected directory
rather than a `subPath`, enable overlay-file mode, and configure the expected
overlay scope. A `ConfigMap` update is not serving evidence until Praxis AI
reports that it accepted the distributed revision.

See [External Client Ingress](external-ingress.md) for the full edge
deployment architecture.

## Reload and rollout

Without delegated mount reconciliation, the operator applies the consumer
Praxis `ConfigMap` on each changed render, but does not automatically restart
gateway pods. With delegation enabled, Grid alternates between the configured
ConfigMap and a Grid-managed slot. It writes the inactive slot before one
Pod-template update switches the config source and required Secret mounts, then
waits for that Deployment rollout to complete.

A Praxis build with file watching applies supported `praxis.yaml` changes
after the mounted file refreshes. Invalid replacements retain the running
pipelines. Listener changes and other startup settings need a rollout; so do
images without file watching:

```console
kubectl rollout restart deployment/praxis-consumer -n <namespace>
```

Check the [Praxis reload reference][praxis-reload] for settings supported by
your image. Mounted Secret changes are separate from `praxis.yaml` changes;
do not assume every filter reloads its credential or certificate files.

Delegated mount reconciliation detects endpoint and Secret reference changes
and Secret rotations. The Deployment owner remains responsible for any
startup-only configuration changes outside that delegation.

The dynamic routing overlay reloads independently. The current `grid-gateway`
also watches `serving-config.json` and the signals pollers' identity files every
five seconds when `GRID_SERVING_CONFIG` is set. Invalid serving-data updates
keep the last accepted settings and topology. Changes to mounted identity files
can still restart signals pollers using those accepted settings. The watcher
does not update listener settings or add load-balancer clusters to `praxis.yaml`.
See the [gateway chart serving guide](../../charts/praxis-gateway/README.md#cross-site-routing-in-agn).

[praxis-reload]: https://github.com/praxis-proxy/praxis/blob/main/docs/operating/configuration.md#dynamic-configuration-reload

## Security

The generated `ConfigMap` never contains credential token bytes.  Credential
entries reference a mounted Kubernetes Secret via a `file:` path.  The Secret
must be provisioned in the cluster where the final-hop gateway or provider-side
component that calls the backend runs.  The
`status.consumerConfigStatus[].message` field also never contains token bytes —
error messages describe structural failures only.
