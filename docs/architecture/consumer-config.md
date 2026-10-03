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
- `plaintext` must not set `sni` (rejected as likely misconfiguration).

## Implemented: GatewayRef.consumerConfig

When `spec.gatewayRefs[].consumerConfig.enabled: true`, the `GridNetwork`
controller renders a `praxis.yaml`-keyed `ConfigMap` in the gateway namespace
when the candidate overlay is nonempty and consumer configuration renders
successfully. The generated config includes:

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

## Operational diagnostics

After enabling `consumerConfig.enabled: true` for a gateway, the `GridNetwork`
status reports the outcome under `status.consumerConfigStatus[]`.

### Delegated mount reconciliation

Secret mount management remains opt in. Set
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
installing the gateway. The first rollout adds Secret mounts while the current
config is still active; after those pods are ready, Grid writes the generated
config and requests its rollout. This also lets a new Deployment become ready
before Grid replaces the bootstrap config.

The operator publishes a reference-only ConfigMap named
`grid-mount-requirements-<first 16 hex characters of SHA-256(configMapName)>`,
with its document under `mount-requirements.json`. It verifies every
referenced Secret and required key in the gateway namespace, and adds only its
reserved volumes and the selected container's mounts. It waits for those mounts
to reach available pods before applying the matching Praxis configuration.
Then it rolls the Deployment for config changes and Secret resource-version
changes. When a reference is removed, the old mount remains until pods with the
new config are ready, then Grid removes only mounts recorded as Grid-owned.
If the last provider disappears, the generated consumer config becomes a
503-only response (with no `intelligent_route` filter); the config rollout
completes before obsolete Grid-owned mounts are pruned. The last overlay
ConfigMap revision remains distributed while candidates are empty because the
Praxis route filter rejects an empty candidates array. Other Deployment fields,
containers, volumes, mounts, and Helm resources are preserved.

All credential, Grid CA, site identity, and custom backend CA Secrets must be
in the gateway namespace. For server-authenticated `tls` endpoints,
`clusterEndpoints[].transport.caSecretRef` can select a custom CA Secret; its
key defaults to `ca.crt`. Mutual TLS uses the Grid CA and site identity from
`GridNetwork.spec.tls`. Secret contents and private keys are never copied into
generated ConfigMaps, status, or logs.

`consumerConfigStatus[].phase: Rendered` means the config map was rendered and
applied. It does not mean the gateway has restarted or become ready. With mount
reconciliation enabled, `mountReconciliationStatus[]` reports
`MountsReconciling`, `WaitingForSecret`, `WaitingForRollout`, `Ready`, or
`Error`. `Ready` requires a completed Deployment rollout with no old replicas;
the selected Praxis container must mount Grid's generated ConfigMap and
`praxis.yaml` key at `/etc/praxis`. Secret resource versions are hashed before
they are placed in pod annotations; Secret values are never used as rollout
metadata.

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
| `MissingSni` | `Error` | A `mutual_tls` cluster endpoint has no (or blank) `sni` — mTLS requires a server name |
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
`mutual_tls` (with `sni`) or `plaintext`.  Add a `transport` block to the
identified endpoint.  The operator will not guess whether a cluster should use
TLS or plaintext.

**Phase is `Error` / reason `MissingSni`**

A `mutual_tls` cluster endpoint has a blank or missing `sni` field.  The `sni`
must match the Subject Alternative Name in the provider gateway's server
certificate.  Add a non-blank `sni` to the endpoint's `transport` block.

**Phase is `Error` / reason `PlaintextWithSni`**

A `plaintext` cluster endpoint has `sni` set.  Setting `sni` on a plaintext
transport does not enable TLS — it is almost certainly a misconfiguration.
Either change the mode to `mutual_tls` (if TLS is intended) or remove `sni`
from the endpoint.

**Consumer pod does not reload generated static configuration**

Praxis gateways do not automatically reload the complete generated Praxis
configuration from a changed `ConfigMap` volume mount. A pod restart, rollout,
or explicit gateway reload is required after the operator updates that static
configuration. The versioned routing overlay is a separate projected file that
`intelligent_route` can validate and hot-reload in process. See
[Reload and rollout](#reload-and-rollout) below.

## Edge-ingress deployments

External edge-ingress gateways reuse the same consumer config contract: the
operator renders a `ConfigMap` with static endpoint topology and `intelligent_route`
candidates, and the edge gateway consumes it the same way a cluster-local
consumer gateway does.

The key distinction for edge deployments is that the routing overlay data
(candidate membership, ordering, freshness) changes more frequently than
static endpoint/TLS topology.  The intended architecture separates these:

- **Static topology** (listener config, endpoint addresses, TLS material,
  filter chain structure): changes require a gateway reload or restart.
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

The operator applies the consumer Praxis `ConfigMap` on every reconcile. Without
delegated mount reconciliation, the consumer gateway pod is not automatically
restarted when the complete generated Praxis configuration changes. With
delegation enabled, Grid waits for required mounts, applies the config, and
waits for the matching Deployment rollout.

To apply updated config to a running consumer pod, restart the `Deployment`:

```console
kubectl rollout restart deployment/praxis-consumer -n <namespace>
```

The dynamic routing overlay can reload independently as described above.
Deployment owners remain responsible for restarting or explicitly reloading
the gateway when static listener or filter-pipeline settings change. Delegated
mount reconciliation also detects endpoint and Secret reference changes and
Secret rotations.

## Security

The generated `ConfigMap` never contains credential token bytes.  Credential
entries reference a mounted Kubernetes Secret via a `file:` path.  The Secret
must be provisioned in the cluster where the final-hop gateway or provider-side
component that calls the backend runs.  The
`status.consumerConfigStatus[].message` field also never contains token bytes —
error messages describe structural failures only.
