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
when consumer configuration renders successfully. The generated config keeps
the watched route filter and complete endpoint inventory even when the current
overlay has no candidates, so a later revision can restore routing.

**Validation status:** `verify-api-fallback-native` proves end-to-end runtime
consumption of the operator-generated `ConfigMap`.  The xtask harness reads the
exact `praxis.yaml` from `op-e2e-consumer-config` in the provider cluster,
applies it byte-for-byte as `praxis-consumer-config` in the consumer cluster, and
confirms all 9 routing assertions pass with the live consumer pod running the
operator-generated config.  Token bytes are absent from the `ConfigMap`,
consumer-cluster replica, overlay JSON, and all logs.

The generated config is a complete Praxis config whose route state is read from
the same versioned overlay ConfigMap Grid publishes for the gateway. The
consumer Deployment must mount that ConfigMap's `routing-overlay.json` key at
`/etc/praxis/routing/routing-overlay.json` as a projected volume (not with
`subPath`) and mount the generated `praxis.yaml` as its Praxis config. For a
cross-cluster consumer, both ConfigMaps must be delivered to the consumer
cluster. The initial migration from previously generated inline candidates
requires a consumer rollout so the new filter configuration and volume mount
take effect.

The generated config contains:

- `listeners:` — one public listener at `0.0.0.0:{listenerPort}` (default 8080)
- `filter_chains:` — the consumer filter chain:
  - `intelligent_route` using `overlay_file`, exact network/gateway/namespace/site
    scope checks, and hot reload. Candidate and selection-policy state is not
    duplicated in startup-only YAML. `provider_hop_clusters` is derived from
    the explicit endpoint inventory: all `mutual_tls` provider-gateway entries
    are included, while plaintext local/dev endpoints are excluded. This lets
    Praxis attach request context required by the provider-side `provider_route`
    filter.
  - a `credential_inject` filter when current routes need credentials or
    `enableProjectedCredentials: true`. In projected mode it starts with an
    empty table and resolves selected references under the projected mount
    root. Missing or invalid files reject with HTTP 503. Token bytes are never
    written to the `ConfigMap`.
  - `load_balancer` entries for every configured endpoint, including currently
    inactive providers needed for restoration. Every potentially routable
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

Both `enableProjectedCredentials` and `supportsProjectedCredentials` default to
`false`. The first generates the filter with an empty credential table even
when there are no credential-bearing candidates. Roll out the consumer, mount
the referenced Secret, and only then set the second field as a readiness
attestation. The Grid controller does not own or restart the consumer
Deployment. Until both are true, it retains credential-bearing revisions and
reports `ProjectedCredentialsUnsupported`. With the capability enabled, later
credential-bearing revisions cannot bypass injection: Praxis resolves the
selected reference or rejects with HTTP 503.

In projected mode, mount each Secret at
`{credentialMountBase}/{secretRef.namespace}/{secretRef.name}` with Secret data
keys as files; Praxis resolves a selected key beneath that directory. This
namespace segment prevents same-name Secrets from aliasing. Existing configured
`file:` sources keep using their explicit paths and are not changed by this
projected-mode contract.

Example overlay mount for a same-cluster consumer (the ConfigMap name is
`grid-overlay-<network>-<gateway>`, subject to the operator's deterministic
name shortening):

```yaml
volumes:
  - name: grid-routing-overlay
    configMap:
      name: grid-overlay-production-inference-gw
      items:
        - key: routing-overlay.json
          path: routing-overlay.json
containers:
  - name: praxis
    volumeMounts:
      - name: grid-routing-overlay
        mountPath: /etc/praxis/routing
        readOnly: true
```

The overlay is the live route authority: a valid empty envelope produces no
route, and malformed replacements retain Praxis's last-known-good snapshot.
Changes to listener, endpoint/TLS topology, or the generated credential
injection table still require the consumer owner to roll/reload Praxis after
the generated `praxis.yaml` changes. That rollout is separate from route-only
overlay reloads. If credentials are present, a restored provider whose
credential reference changed requires this config rollout before its request
can succeed when using a static credential table; `credential_inject` fails
closed when the configured reference does not match the overlay. In
projected-credential mode, a changed reference needs no config rollout when
the credential filter is already running and the matching Secret is mounted
under `{credentialMountBase}/{namespace}/{name}`. If that Secret projection
is not mounted, the request fails closed until the consumer installs it.

See [`docs/architecture/crds.md`](crds.md#gatewayrefconsumerconfig) for the full
field reference.

## Grid-managed live-routing consumers

Grid has three live candidate consumers. Their route state is not equivalent to
an empty list in arbitrary startup YAML:

| Consumer path | Route-state source | Valid no-route state | Runtime update behavior |
|---|---|---|---|
| Praxis `intelligent_route` with `overlay_file` | Grid's versioned `routing-overlay.json` ConfigMap | A valid versioned envelope whose `overlay.candidates` is `[]` | Praxis validates and atomically serves the new snapshot; malformed replacements retain last-known-good. |
| Generated `GatewayRef.consumerConfig` | The same scoped versioned overlay; generated `praxis.yaml` supplies filter and endpoint plumbing | The same empty envelope | Candidate-only updates hot reload. Credential-bearing revisions are held until `enableProjectedCredentials` has been rolled out and `supportsProjectedCredentials: true` attests the filter and Secret mounts are active. Listener, endpoint/TLS, and filter-pipeline changes still require the consumer owner to reload or roll out its Praxis configuration. |
| Embedded `grid-gateway` `grid_site_route` filter | The operator-published `grid-serving-<network>-<gateway>` ConfigMap | A valid serving config with `candidates: []` | The running gateway watches the projected serving file and atomically replaces its candidate snapshot and provider-hop allowlist. Malformed updates retain the previous snapshot. Provider-hop trust is declared separately with `GatewayRef.providerHopEndpoints`. |

Grid publishes authoritative empty revisions without a capability flag. Every
consumer of a gateway's overlay must therefore run an image with empty-snapshot
support before this Grid version is deployed. The paired AI change and a
compatible image release are prerequisites for the next Grid release.

The embedded gateway's filter chain and upstream cluster definitions remain in
its startup Praxis configuration, but its changing candidate list is not a
startup-only inline list: it comes from the watched Grid serving ConfigMap.
Its provider-hop allowlist comes from the separate
`GatewayRef.providerHopEndpoints` field, not from `consumerConfig`; each entry
must declare `mutual_tls` and a nonblank SNI matching the embedded gateway's
verified upstream configuration. The embedded gateway carries the Grid
overlay's stable candidate ID and generates a fresh hop request ID.
Caller-supplied routing-context headers are removed before forwarding. The provider gateway still authenticates
the peer with mTLS before consuming that context.

Static, manually configured `intelligent_route.candidates` remain a distinct
contract: an empty static list is rejected, and these immutable candidate lists
do not receive Grid's runtime-withdrawal semantics. The operator's
`overlay_bridge` helper is currently a conversion/test utility, not an active
controller consumer. Grid-managed runtime withdrawal must use one of the three
live paths above; publishing an empty overlay does not change an unrelated
static Praxis configuration.

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
Praxis acceptance of populated routes. Older generated configs embedded
`admission_state` and `selection_group` in inline candidates, which the
previously supported Praxis AI image rejected. Populated routes require a
Praxis AI image that accepts Grid's versioned-overlay config. They also require
the versioned-overlay config in
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
routing overlay. The generated consumer config retains `intelligent_route`, its
overlay watcher, and the complete endpoint inventory so a later revision can
restore routing. For delegated mounts, the empty overlay is published before
config reconciliation, so consumers using the watched overlay stop selecting
providers even if config reconciliation fails.
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
| `ProjectedCredentialsUnsupported` | `Error` | A credential-bearing overlay is retained until the consumer declares that its projected credential filter is already running |
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
authoritative routing overlay. The generated consumer `ConfigMap` retains the
watched `intelligent_route` filter and configured endpoints for restoration.
The ConfigMap update does not itself restart gateway pods; use a supported file
reload or restart when the running gateway does not reload it.

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

For operator-generated consumer configs, `intelligent_route.provider_hop_clusters`
is derived from the explicit endpoint inventory: every
`clusterEndpoints[]` entry with `transport.mode: mutual_tls` is included, while
explicit plaintext local/dev endpoints are not. This ensures requests routed
through the authenticated provider-gateway hop carry the candidate and request
context required by the provider's `provider_route` filter. The setting is
retained when candidate and selection state moves into the versioned overlay.

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
