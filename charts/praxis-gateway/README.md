# Praxis Gateway Helm Chart (Temporary)

Temporary workload chart that deploys the Praxis AI gateway process directly
as a Kubernetes Deployment. This chart currently lives in the
`praxis-proxy/grid` repository because
no Praxis/Gateway Operator or supported Kubernetes installation path exists
yet.

**This chart is not an operator.** It does not define CRDs, controllers, or
dynamic discovery. It mounts a supplied Praxis configuration and optional
TLS, overlay, and credential Secrets.

**Ownership:** Temporary integration chart in this repository. Long-term ownership moves to
the future Praxis/Gateway Operator repository when that deployment API and
release ownership exist. Do not treat the `praxis-gateway` chart as a permanent
responsibility of this repository.

## Prerequisites

- Kubernetes >= 1.26
- Helm >= 3.12
- A way to provide `praxis.yaml`, see [Where praxis.yaml comes from](#where-praxisyaml-comes-from)
- A compatible Praxis AI gateway image (default: Praxis AI 0.4.0)

## Install

From a local checkout:

```bash
kubectl create configmap edge-gateway-config \
  --from-file=praxis.yaml=path/to/praxis.yaml \
  -n grid-system

helm install edge-gateway charts/praxis-gateway \
  --namespace grid-system \
  --set praxisConfig.configMapName=edge-gateway-config
```

### Where praxis.yaml comes from

`praxisConfig.source` picks who writes the gateway's `praxis.yaml`: `byo`
(default), `operator`, or `render`. Each part below shows the minimal settings.

#### byo: you write praxis.yaml

Create the ConfigMap with a `praxis.yaml` key, then point the chart at it:

```bash
kubectl create configmap my-praxis-config --from-file=praxis.yaml=./praxis.yaml -n grid-system
```

```yaml
praxisConfig:
  source: byo
  configMapName: my-praxis-config
```

Set `praxisConfig.key` only when your ConfigMap uses a key other than
`praxis.yaml`.

To terminate TLS at the gateway, or to trust a private CA for backend TLS, the
chart mounts the Secrets and your praxis.yaml points at the files:

```yaml
listenerTls:
  secretName: my-listener-cert   # Secret with tls.crt and tls.key
upstreamCA:
  secretName: my-ca              # Secret with ca.crt
```

```yaml
# in your praxis.yaml
runtime:
  upstream_ca_file: /etc/praxis/upstream-ca/ca.crt
listeners:
  - name: gateway
    address: "0.0.0.0:8080"
    tls:
      certificates:
        - cert_path: /etc/praxis/listener-tls/tls.crt
          key_path: /etc/praxis/listener-tls/tls.key
```

#### operator: the Grid operator writes praxis.yaml

Enable `consumerConfig` for this gateway in the GridNetwork:

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridNetwork
metadata:
  name: my-network
spec:
  gatewayRefs:
    - name: edge-gateway
      namespace: grid-system
      consumerConfig:
        enabled: true
```

```yaml
praxisConfig:
  source: operator
```

The operator writes the ConfigMap `praxis-consumer-config`, and the pod starts
once it exists. If the GridNetwork sets another `consumerConfig.configMapName`,
set the same name in `praxisConfig.configMapName`.
Every provider cluster the gateway routes to also needs a
`consumerConfig.clusterEndpoints` entry, or the operator does not write the
ConfigMap.

`listenerTls` and `upstreamCA` are not supported in this mode: the install
fails, because the operator's praxis.yaml uses neither. Terminate TLS in front
of the gateway.

#### render: this chart writes praxis.yaml

```yaml
grid:
  siteName: us-east
praxisConfig:
  source: render
  render:
    model: my-model
    auth:
      mode: none
    backends:
      - cluster: vllm
        endpoints: ["10.0.0.5:8000"]
```

`grid.siteName`, `model`, `auth.mode` and at least one backend are required. `auth.mode: none`
has no caller authentication, so use it only behind an authenticating front. Use
`api-key` with `auth.validateUrl` to check caller keys.

The default image reference is the official Praxis AI 0.4.0 gateway tag.
`image.digest` defaults to empty so an `image.tag` override remains effective.
For immutable deployments, set `image.digest` explicitly to
`sha256:0f619d4a0b533093f94a76921cfbba0ecdec51557dffee1615a29721ee1fc878`.
Praxis AI 0.4.0 depends on Praxis core 0.7.0; these are separate release
versions.

The standard Praxis AI 0.4.0 image supports AGN provider selection and load
balancing and includes Basic Auth. It does not include the optional
`token-rate-limit-filter` required by the distributed token quota qualification.
That qualification is not supported by this default image; AGN does not
publish a replacement AI rollup.

The chart uses [Semantic Versioning](https://semver.org/). Its `version`
identifies the chart package, while `appVersion` identifies the default Praxis
AI image; these values may advance independently.

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `replicaCount` | int | `1` | Gateway replicas. |
| `image.repository` | string | `ghcr.io/praxis-proxy/ai` | Image repository. |
| `image.tag` | string | `0.4.0` | Image tag (ignored when `image.digest` is set). |
| `image.digest` | string | `""` | Immutable digest (sha256:…). When set, tag is ignored. |
| `image.pullPolicy` | string | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | list | `[]` | Pull secrets for private registries. |
| `nameOverride` | string | `""` | Override chart name. |
| `fullnameOverride` | string | `""` | Override fully qualified app name. |
| `commonLabels` | object | `{}` | Labels added to all resources. |
| `podLabels` | object | `{}` | Additional pod labels. Selector labels cannot be overridden. |
| `podAnnotations` | object | `{}` | Pod annotations. |
| `podSecurityContext` | object | `{}` | Extra pod securityContext (`runAsUser`, `runAsGroup`, `fsGroup`, `supplementalGroups`). |
| `args` | list | `["--config", "/etc/praxis/praxis.yaml"]` | Container arguments. |
| `grid.networkName` | string | `""` | GridNetwork name. Used only by the overlay sidecar, which refuses an overlay written for another GridNetwork. Required when `overlay.sidecar.enabled`. Must match the GridNetwork CR name. |
| `grid.siteName` | string | `""` | This gateway's site. Used by `render` (written as `local_site`) and by the overlay sidecar (refuses an overlay written for another site). Required with `source: render` or `overlay.sidecar.enabled`. |
| `praxisConfig.source` | string | `byo` | Who writes praxis.yaml: `byo` (you create the ConfigMap), `operator` (the Grid operator creates it from a GridNetwork `gatewayRef` with `consumerConfig.enabled`), or `render` (this chart creates it from `praxisConfig.render`). |
| `praxisConfig.configMapName` | string | **required** for `byo`; `praxis-consumer-config` for `operator` | Name of the ConfigMap with praxis.yaml. For `operator`, must match the `configMapName` in the GridNetwork. Not used with `render`. |
| `praxisConfig.key` | string | `praxis.yaml` | Key in the ConfigMap, for `byo` only. `operator` always uses `praxis.yaml`. |
| `praxisConfig.render.model` | string | **required** when rendered | Model advertised on the routing candidates. |
| `praxisConfig.render.backends` | list | **required** when rendered | Backend clusters (`cluster`, `endpoints`, `healthCheck`, `transport`). |
| `praxisConfig.render.backends[].transport` | object | `mutual_tls` with `gridIdentity.secretName`, else `plaintext` | `mode`: `mutual_tls` presents the grid identity, `tls` verifies the server cert with no client cert, `plaintext` is cleartext. `sni` names the peer cert (required for `mutual_tls` and for `tls` to an IP endpoint). `ca` (`configMapName` or `secretName`, `key`) is the CA for a `tls` backend. A `tls` backend trusts, first match wins: `transport.ca`, then `upstreamCA`, then the process store, which is the `auth.validateCA` bundle when that is set. |
| `praxisConfig.render.auth.mode` | string | **required** when rendered | `api-key` validates the caller's key and needs an image that registers `identity/api-key` (praxis-policy 0.4 or later); the render refuses it on the default `ai:0.4.0` image (by effective reference; a digest pin of that same image is not detected). `none` renders no policy filter, for use only behind an authenticating front. |
| `praxisConfig.render.auth.allowUnauthenticatedExposure` | bool | `false` | With `none`, allow a LoadBalancer or NodePort Service. Without it the render fails. The guard sees only this chart's Service, not `oc expose`, another Service selecting the pod labels, an HTTPRoute, or a hand-made Service with `service.enabled=false`. Use `networkPolicy` for those. |
| `praxisConfig.render.auth.stripAuthorization` | bool | `true` | Remove the caller's `Authorization` before routing, in either mode. Forwarded grid hops authenticate by mTLS identity. `false` forwards the caller's key or bearer to every backend and cross-site peer, so use it only when the backend validates that same credential. |
| `praxisConfig.render.auth.validateUrl` | string | **required** for `api-key` | https validate endpoint. |
| `praxisConfig.render.auth.allowPrivateEndpoint` | bool | `false` | Sets `allow_private_idp`, which is engine-wide: every policy callout may reach private, loopback, and link-local addresses, not only `validateUrl`. |
| `praxisConfig.render.auth.validateCA` | object | empty | CA for the validate call (`configMapName` or `secretName`, `key`). Set as `SSL_CERT_FILE`, which replaces the platform trust store for the validate call and https backends without a per-backend CA or `upstreamCA`. mutual_tls backends and `upstreamCA` are unaffected. See the recipe below. |
| `networkPolicy.enabled` | bool | `false` | Render a NetworkPolicy that limits which pods can reach the listener port, where the CNI enforces NetworkPolicy. It is not authentication. Node and host-network traffic handling is CNI-specific (OVN-Kubernetes: the `policy-group.network.openshift.io/host-network` label), and a LoadBalancer with `externalTrafficPolicy: Cluster` can SNAT clients to node IPs. |
| `networkPolicy.from` | list | `[]` | NetworkPolicyPeer entries allowed in. Required when enabled. With `auth.mode: none`, list only the authenticating front. `{podSelector: {}}` admits every pod in this namespace. An empty `namespaceSelector` and an `ipBlock` of `0.0.0.0/0` or `::/0` admit everyone and fail the render. An all-address `ipBlock` with `except` entries is allowed. The check reads selector emptiness and the cidr only, so `matchExpressions` that happen to select every pod pass. A provider gateway behind a LoadBalancer that SNATs clients to node IPs needs `ipBlock` peers for those node addresses. |
| `listenerTls.secretName` | string | `""` | Server cert Secret (`tls.crt`, `tls.key`) for TLS on the listener. Set it to turn listener TLS on; empty (the default) turns it off. Works with `render` and `byo`; `render` writes the listener cert paths, a byo config must point its listener `cert_path`/`key_path` at `listenerTls.mountPath` (`/etc/praxis/listener-tls`). Names the port `https`. Not supported with `operator`: the install fails. On OpenShift, annotate the Service with `service.beta.openshift.io/serving-cert-secret-name`. |
| `upstreamCA.secretName` | string | `""` | CA bundle Secret for backend TLS without a per-cluster CA. Set it to turn it on; empty (the default) turns it off. Works with `render` and `byo`; `render` writes `upstream_ca_file`, a byo config must set `runtime.upstream_ca_file` to `upstreamCA.mountPath`/`upstreamCA.key` (`/etc/praxis/upstream-ca/ca.crt`). Not supported with `operator`: the install fails. |
| `port.containerPort` | int | `8080` | Container port. |
| `port.name` | string | `""` | Port name. Empty: `https` when `listenerTls.secretName` is set, else `http`. |
| `port.protocol` | string | `TCP` | Port protocol. |
| `service.enabled` | bool | `true` | Create a Service. |
| `service.type` | string | `ClusterIP` | Service type. |
| `service.port` | int | `8080` | Service port. |
| `service.annotations` | object | `{}` | Service annotations. |
| `service.loadBalancerIP` | string | `""` | Static IP for LoadBalancer. |
| `overlay.configMapName` | string | `""` | Name of the overlay ConfigMap. Set it to mount the overlay; empty (the default) turns it off. |
| `overlay.mountPath` | string | `/etc/praxis/routing` | Mount path for overlay files. |
| `overlay.items` | list | routing-overlay.json | Items to project. |
| `overlay.sidecar.enabled` | bool | `false` | Deliver validated overlays through an API-watch sidecar instead of kubelet ConfigMap projection. |
| `overlay.sidecar.image.repository` | string | `grid-overlay-sync` | Overlay-sync image repository. Use a published or locally built image appropriate to the deployment. |
| `overlay.sidecar.image.tag` | string | `latest` | Overlay-sync image tag. Use an immutable published tag for reproducible deployments. |
| `overlay.sidecar.image.pullPolicy` | string | `IfNotPresent` | Overlay-sync image pull policy. |
| `overlay.sidecar.dataKey` | string | `routing-overlay.json` | Content-addressed envelope key in the overlay ConfigMap. |
| `overlay.sidecar.resources` | object | small requests and limits | Resources for both the one-shot init container and continuous sidecar. |
| `gridIdentity.secretName` | string | `""` | Grid identity Secret (`tls.crt`, `tls.key`, `ca.crt`) for mTLS to backends and peers. Set it to turn the grid identity on; empty (the default) turns it off. You create the Secret. Works with every `praxisConfig.source`. `operator`: each GridNetwork `consumerConfig.clusterEndpoints[].transport.mode` picks `MutualTls` (uses this mount) or `Plaintext`. `byo`: your praxis.yaml decides; point its cluster `tls` block at `gridIdentity.mountPath`. `render`: each `render.backends[].transport.mode` picks `mutual_tls`, `tls` or `plaintext`; with no mode, `mutual_tls` when `secretName` is set, else `plaintext`. |
| `gridIdentity.mountPath` | string | `/etc/praxis/tls` | Mount path for TLS files. |
| `providerCredentials` | list | `[]` | Provider credential Secrets to mount (`secretName`, `mountPath`, `optional`): one entry for each Secret referenced in an InferenceProvider CR's `spec.auth.secretRef`. You create the Secrets, in the release namespace. `mountPath` defaults to `/run/secrets/grid-credentials/<secretName>`, the operator's default `credentialMountBase`; with `byo`, set it to the path your praxis.yaml uses. |
| `health.readiness` | object | TCP socket on the listener port | Readiness probe. A `tcpSocket` without a port targets the listener port. Set to null to disable. |
| `health.liveness` | object | TCP socket on the listener port | Liveness probe. Set to null to disable. |
| `resources` | object | `{}` | Container resource requests and limits. |
| `nodeSelector` | object | `{}` | Node selector. |
| `affinity` | object | `{}` | Pod affinity rules. |
| `tolerations` | list | `[]` | Pod tolerations. |
| `topologySpreadConstraints` | list | `[]` | Topology spread constraints. |
| `priorityClassName` | string | `""` | Pod priority class. |

### KServe backend on OpenShift

A KServe LLMInferenceService serves HTTPS on :8000 with a cert from the OpenShift
service CA. Use the workload Service ClusterIP as the endpoint: praxis refuses a
hostname that resolves to a private address. Set `sni` to the Service DNS name, which
the cert carries, and trust the service CA that OpenShift injects into every namespace:

```yaml
praxisConfig:
  source: render
  render:
    backends:
      - cluster: local-qwen3
        endpoints: ["172.30.12.34:8000"]   # kubectl get svc qwen3-kserve-workload-svc -o jsonpath='{.spec.clusterIP}'
        transport:
          mode: tls
          sni: qwen3-kserve-workload-svc.llm.svc
          ca: { configMapName: openshift-service-ca.crt, key: service-ca.crt }
```

The health check defaults to `tcp` for TLS backends.

### validateCA bundle recipe

The bundle replaces the platform store, so include the image's roots with the private CA:

```sh
podman run --rm --entrypoint cat <gateway image> /etc/ssl/certs/ca-certificates.crt > bundle.pem
cat service-ca.crt >> bundle.pem
kubectl create configmap gateway-validate-ca --from-file=ca.crt=bundle.pem
```

Then set `praxisConfig.render.auth.validateCA.configMapName=gateway-validate-ca`. If only the validate
call and mutual_tls backends make TLS calls, the service CA alone is enough. Public https
backends can instead take `upstreamCA`.

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
gateway may continue serving an old preference even though AGN has already
published a new overlay.

Enable `grid-overlay-sync` when prompt routing convergence matters:

```text
AGN Operator updates ConfigMap
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
grid:
  networkName: production
  siteName: us-east-edge
overlay:
  configMapName: grid-overlay-production-consumer-gateway
  mountPath: /etc/praxis/routing
  sidecar:
    enabled: true
    image:
      repository: registry.example.com/grid-overlay-sync
      tag: <version>
      pullPolicy: IfNotPresent
    dataKey: routing-overlay.json
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

This mechanism removes kubelet projection latency only after AGN applies a
ConfigMap. Total route-change time still includes metrics publication, the
provider scrape, AGN reconciliation, ConfigMap application, sidecar delivery,
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
- Mounts an overlay ConfigMap from the AGN Operator
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

The AGN Operator's `gateway.serviceName` must match the consumer
gateway's Service name. When using `fullnameOverride`, set
`gateway.serviceName` to the same value in the operator Helm values.
