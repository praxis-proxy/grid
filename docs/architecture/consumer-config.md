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
every reconcile. To safely add credential-bearing routes, first enable
`enableProjectedCredentials`, roll out the consumer with that generated filter,
then set `supportsProjectedCredentials` after the Secret mount is ready. The
generated config includes:

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
    endpoint address and explicit `transport` configuration (`mutual_tls` or
    `plaintext`).  Missing transport fails closed — the operator will not
    silently render a plain-HTTP cluster when transport intent is absent
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

This generated config covers the direct API-provider path where the consumer
gateway is often also the final-hop gateway for the provider API call.  Remote
provider sites follow the same SecretRef contract, but the provider credential
should be mounted only where the final backend call is made.

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
support before this Grid version is deployed. The chart-default Praxis AI 0.4.0
image is not compatible; the paired AI change and a compatible image release
are prerequisites for the next Grid release.

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

The operator applies the consumer Praxis `ConfigMap` on every reconcile. The
consumer gateway pod is not owned by the operator and is not automatically
restarted when the complete generated Praxis configuration changes.

To apply updated config to a running consumer pod, restart the `Deployment`:

```console
kubectl rollout restart deployment/praxis-consumer -n <namespace>
```

The dynamic routing overlay can reload independently as described above.
Deployment owners remain responsible for restarting or explicitly reloading
the gateway when static listener, filter-pipeline, endpoint/TLS topology, or
mounted Secret content changes.

## Security

The generated `ConfigMap` never contains credential token bytes.  Credential
entries reference a mounted Kubernetes Secret via a `file:` path.  The Secret
must be provisioned in the cluster where the final-hop gateway or provider-side
component that calls the backend runs.  The
`status.consumerConfigStatus[].message` field also never contains token bytes —
error messages describe structural failures only.
