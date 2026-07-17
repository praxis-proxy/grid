# Operator-Generated Consumer Config

The Grid operator can generate the consumer Praxis `ConfigMap` from routing overlay
data.  This is an opt-in feature on each `GatewayRef`.

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
  - `grid_route` candidates from the overlay (with `credential.secretRef` for
    credential-bearing candidates)
  - `grid_credential_inject` entries using `file:` sources when credential-bearing
    candidates are present — token bytes are never written to the `ConfigMap`
  - `load_balancer` entries (one per unique candidate cluster). Every referenced
    cluster must have a matching `consumerConfig.clusterEndpoints[]` entry with
    endpoint and optional TLS settings
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

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
| `ConsumerConfigRenderFailed` | `Error` | Overlay data produced an unrenderable config (e.g. blank local site) |
| `ConsumerConfigApplyFailed` | `Error` | Kubernetes API rejected the `ConfigMap` apply (e.g. RBAC, namespace not found) |
| `ConsumerConfigError` | `Error` | Other error during render or apply |

### Troubleshooting

**Phase is `Error` / reason `ConsumerConfigApplyFailed`**

The operator could not apply the `ConfigMap`.  Common causes:

- Missing RBAC: the operator's `ServiceAccount` lacks `configmaps.create` /
  `configmaps.update` in the gateway namespace.  See the
  [RBAC requirements](operations.md#consumer-config-rbac) in the operations guide.
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

**Consumer pod does not reload when the ConfigMap changes**

Praxis gateways do not automatically hot-reload from a changed ConfigMap volume
mount.  A pod restart or rollout is required after the operator updates the
`ConfigMap`.  See [Reload and rollout](#reload-and-rollout) below.

## Reload and rollout

The operator applies the consumer Praxis `ConfigMap` on every reconcile.  The
consumer gateway pod is not owned by the operator and is not automatically
restarted when the `ConfigMap` changes.

To apply updated config to a running consumer pod, restart the `Deployment`:

```console
kubectl rollout restart deployment/praxis-consumer -n <namespace>
```

Automatic reload support is outside the current operator contract.  Deployment
owners are responsible for restarting or reloading the gateway when generated
config or mounted Secret content changes.

## Security

The generated `ConfigMap` never contains credential token bytes.  Credential
entries reference a mounted Kubernetes Secret via a `file:` path.  The Secret must
be provisioned in the consumer cluster by external tooling.  The
`status.consumerConfigStatus[].message` field also never contains token bytes —
error messages describe structural failures only.
