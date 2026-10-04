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
- `transport.mode` is the security switch (`mutual_tls` or explicit
  insecure/dev-only `plaintext`), not `sni` presence.
- Missing `transport` fails closed — the operator will not render the cluster entry.
- `plaintext` must not set `sni` (rejected as likely misconfiguration).

## Implemented: GatewayRef.consumerConfig

When `spec.gatewayRefs[].consumerConfig.enabled: true`, the `GridNetwork`
controller renders a `praxis.yaml`-keyed `ConfigMap` in the gateway namespace on
every reconcile.  The generated config includes:

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
    endpoint address and explicit `transport` configuration (`mutual_tls` or
    `plaintext`).  Missing transport fails closed — the operator will not
    silently render a plain-HTTP cluster when transport intent is absent
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

This generated config covers the direct API-provider path where the consumer
gateway is often also the final-hop gateway for the provider API call.  Remote
provider sites follow the same SecretRef contract, but the provider credential
should be mounted only where the final backend call is made.

The generated config requires a Praxis AI image that contains the
`credential_inject` filter. AGN can render the config and project
credential references today; deployments must ensure the selected Praxis AI image
includes the matching request-time filter.

See [`docs/architecture/crds.md`](crds.md#gatewayrefconsumerconfig) for the full
field reference.

## Operational diagnostics

After enabling `consumerConfig.enabled: true` for a gateway, the `GridNetwork`
status reports the outcome under `status.consumerConfigStatus[]`.

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

**Consumer pod has not applied an updated Praxis ConfigMap**

The operator updates the ConfigMap; it does not restart gateway pods. A Praxis
build with file watching reloads supported routes, filter pipelines, and
load-balancer endpoints after the kubelet refreshes the mounted file. Mount the
directory rather than a `subPath`, and check gateway logs for acceptance.
Listener and other startup settings still require a restart. The routing
overlay is a separate file that `intelligent_route` validates and reloads. See
[Reload and rollout](#reload-and-rollout) below.

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

The operator applies the consumer Praxis `ConfigMap` on every reconcile. The
consumer gateway pod is not owned by the operator and is not automatically
restarted when the complete generated Praxis configuration changes.

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
