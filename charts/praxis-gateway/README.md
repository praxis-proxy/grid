# Praxis Gateway Helm Chart (Temporary)

Temporary workload chart that deploys the Praxis AI gateway process directly
as a Kubernetes Deployment. This chart exists in the Grid repository because
no Praxis/Gateway Operator or supported Kubernetes installation path exists
yet.

**This chart is not an operator.** It does not define CRDs, controllers, or
dynamic discovery. It mounts a supplied Praxis configuration and optional
TLS, overlay, and credential Secrets.

**Ownership:** Temporary Grid integration asset. Long-term ownership moves to
the future Praxis/Gateway Operator repository when that deployment API and
release ownership exist. Do not treat this as a permanent Grid responsibility.

## Prerequisites

- Kubernetes >= 1.26
- Helm >= 3.12
- A Praxis configuration ConfigMap already created in the target namespace
- A compatible Praxis AI gateway image (default: Praxis AI 0.3.0)

## Install

From a local checkout:

```bash
kubectl create configmap edge-gateway-config \
  --from-file=praxis.yaml=path/to/praxis.yaml \
  -n grid-system

helm install edge-gateway charts/praxis-gateway \
  --namespace grid-system \
  --set config.existingConfigMap=edge-gateway-config
```

The default image is the official Praxis AI 0.3.0 gateway. Override
`image.repository`, `image.tag`, or `image.digest` to install another compatible
gateway image. Prefer a digest when reproducing a validated deployment.

The standard Praxis AI 0.3.0 image supports Grid provider selection and load
balancing. It does not include the optional `token-rate-limit-filter` and
`praxis-filter/basic-auth-filter` features required by the distributed token
quota qualification. That qualification requires an explicitly supplied Praxis
AI image built with both features; Grid does not publish a replacement AI
rollup.

The chart uses [Semantic Versioning](https://semver.org/). Its `version`
identifies the chart package, while `appVersion` identifies the default Praxis
AI image; these values may advance independently.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `replicaCount` | int | `1` | Gateway replicas. |
| `image.repository` | string | `ghcr.io/praxis-proxy/ai` | Image repository. |
| `image.tag` | string | `0.3.0` | Image tag. |
| `image.digest` | string | `""` | Immutable digest (sha256:…). When set, tag is ignored. |
| `image.pullPolicy` | string | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | list | `[]` | Pull secrets for private registries. |
| `nameOverride` | string | `""` | Override chart name. |
| `fullnameOverride` | string | `""` | Override fully qualified app name. |
| `commonLabels` | object | `{}` | Labels added to all resources. |
| `podLabels` | object | `{}` | Additional pod labels. Selector labels cannot be overridden. |
| `podAnnotations` | object | `{}` | Pod annotations. |
| `terminationGracePeriodSeconds` | integer | `null` | Optional maximum termination grace period for gateway pods. Qualification topologies set this explicitly. |
| `podSecurityContext` | object | `{}` | Extra pod securityContext (`runAsUser`, `runAsGroup`, `fsGroup`, `supplementalGroups`). |
| `args` | list | `["--config", "/etc/praxis/praxis.yaml"]` | Container arguments. |
| `config.existingConfigMap` | string | **required** | Name of an existing ConfigMap with the Praxis config. |
| `config.key` | string | `praxis.yaml` | Key in the ConfigMap. |
| `port.containerPort` | int | `8080` | Container port. |
| `port.name` | string | `http` | Port name. |
| `port.protocol` | string | `TCP` | Port protocol. |
| `service.enabled` | bool | `true` | Create a Service. |
| `service.type` | string | `ClusterIP` | Service type. |
| `service.port` | int | `8080` | Service port. |
| `service.annotations` | object | `{}` | Service annotations. |
| `service.loadBalancerIP` | string | `""` | Static IP for LoadBalancer. |
| `overlay.enabled` | bool | `false` | Mount an overlay ConfigMap. |
| `overlay.existingConfigMap` | string | `""` | Name of the overlay ConfigMap. |
| `overlay.mountPath` | string | `/etc/praxis/routing` | Mount path for overlay files. |
| `overlay.items` | list | routing-config.json, routing-overlay.json | Items to project. |
| `overlay.sidecar.enabled` | bool | `false` | Deliver validated overlays through an API-watch sidecar instead of kubelet ConfigMap projection. |
| `overlay.sidecar.image.repository` | string | `grid-overlay-sync` | Overlay-sync image repository. Use a published or locally built image appropriate to the deployment. |
| `overlay.sidecar.image.tag` | string | `latest` | Overlay-sync image tag. Use an immutable published tag for reproducible deployments. |
| `overlay.sidecar.image.pullPolicy` | string | `IfNotPresent` | Overlay-sync image pull policy. |
| `overlay.sidecar.dataKey` | string | `routing-overlay.json` | Content-addressed envelope key in the overlay ConfigMap. |
| `overlay.sidecar.expectedNetwork` | string | `""` | Required GridNetwork scope when the sidecar is enabled. |
| `overlay.sidecar.expectedLocalSite` | string | `""` | Required local-site scope when the sidecar is enabled. |
| `overlay.sidecar.resources` | object | small requests and limits | Resources for both the one-shot init container and continuous sidecar. |
| `tls.enabled` | bool | `false` | Mount a TLS Secret. |
| `tls.existingSecret` | string | `""` | Name of the TLS Secret. |
| `tls.mountPath` | string | `/etc/praxis/tls` | Mount path for TLS files. |
| `credentials` | list | `[]` | Credential Secret mounts (name, mountPath, optional). |
| `health.readiness` | object | TCP socket on port `http` | Readiness probe. Set to null to disable. |
| `health.liveness` | object | TCP socket on port `http` | Liveness probe. Set to null to disable. |
| `resources` | object | `{}` | Container resource requests and limits. |
| `nodeSelector` | object | `{}` | Node selector. |
| `affinity` | object | `{}` | Pod affinity rules. |
| `tolerations` | list | `[]` | Pod tolerations. |
| `topologySpreadConstraints` | list | `[]` | Topology spread constraints. |
| `priorityClassName` | string | `""` | Pod priority class. |

## Security

The chart enforces Kubernetes restricted security defaults:

- `runAsNonRoot: true` (no fixed UID)
- `readOnlyRootFilesystem: true`
- `allowPrivilegeEscalation: false`
- All Linux capabilities dropped
- `seccompProfile.type: RuntimeDefault`
- `automountServiceAccountToken: false`

When overlay-sync is enabled, the pod uses a dedicated ServiceAccount, but
automatic token mounting remains disabled. A short-lived projected token is
mounted only into the overlay-sync init and sidecar containers. The Praxis
container has no Kubernetes API credential and mounts the delivered overlay
directory read-only.

## Routing Overlay Delivery

Praxis can hot-reload a routing overlay as soon as its file changes. A normal
ConfigMap volume, however, is updated by the kubelet on an eventual refresh
cycle. That delay can be longer than a temporary provider-pressure event, so a
gateway may continue serving an old preference even though Grid has already
published a new overlay.

Enable `grid-overlay-sync` when prompt routing convergence matters:

```text
Grid operator updates ConfigMap
             |
             | Kubernetes API watch
             v
      overlay-sync sidecar
        validate envelope
        atomic file replace
        retain last-known-good on failure
             |
             | shared emptyDir
             v
       Praxis hot reload
```

Example values:

```yaml
overlay:
  enabled: true
  existingConfigMap: grid-overlay-production-consumer-gateway
  mountPath: /etc/praxis/routing
  sidecar:
    enabled: true
    image:
      repository: registry.example.com/grid-overlay-sync
      tag: <version>
      pullPolicy: IfNotPresent
    dataKey: routing-overlay.json
    expectedNetwork: production
    expectedLocalSite: us-east-edge
```

When enabled, the chart creates:

- an `overlay-sync-init` init container that waits for the first valid overlay
  before Praxis starts;
- an `overlay-sync` sidecar that watches one named ConfigMap;
- a shared `emptyDir` used for atomic file publication;
- a dedicated ServiceAccount, Role, and RoleBinding; and
- sidecar readiness and liveness probes on port `9091`.

The sidecar validates maximum size, schema version, destination scope,
content-addressed revision, and SHA-256 digest. Invalid replacements do not
touch the serving file. ConfigMap deletion or temporary API loss marks the
sidecar degraded while retaining the last-known-good overlay.

This mechanism removes kubelet projection latency only after Grid applies a
ConfigMap. Total route-change time still includes metrics publication, the
provider scrape, Grid reconciliation, ConfigMap application, sidecar delivery,
and Praxis hot reload. Overlay-sync does not change the scrape or reconcile
intervals.

With `overlay.sidecar.enabled: false`, the chart retains the simpler direct
ConfigMap mount. Use that compatibility mode for static configuration or when
kubelet-controlled refresh latency is acceptable.

## Edge vs Provider Gateway

The chart is role-neutral. Edge and provider gateways use the same chart
with different values:

**Edge gateway:**
- Listens on port 8080 (HTTP)
- Mounts an overlay ConfigMap from the Grid operator
- Mounts a TLS Secret for upstream connections

**Provider gateway:**
- Listens on port 8443 (mTLS)
- Mounts a TLS Secret for client authentication
- Mounts credential Secrets for backend provider access
- Helm release name must match the mock-providers
  `networkPolicy.providerGateway.instanceLabel` (default: `provider-gateway`)
  so the NetworkPolicy allows traffic

## Resource Names

The chart's fullname template produces `{release}-praxis-gateway` by
default (e.g., release `consumer-gateway` → Service name
`consumer-gateway-praxis-gateway`). Set `fullnameOverride` to control
the exact Service name:

```yaml
fullnameOverride: consumer-gateway   # Service name = consumer-gateway
```

The Grid Operator's `gateway.serviceName` must match the consumer
gateway's Service name. When using `fullnameOverride`, set
`gateway.serviceName` to the same value in the operator Helm values.
