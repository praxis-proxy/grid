# grid-operator Helm Chart

Helm chart for deploying the `grid-operator` multi-site AI inference routing
controller on Kubernetes.

## Prerequisites

- Kubernetes >= 1.26
- Helm >= 3.12

## Install

By semantic version (OCI):

```bash
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <version> \
  --namespace grid-system \
  --create-namespace
```

By immutable digest (requires Helm >= 3.13):

```bash
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator@sha256:<digest> \
  --namespace grid-system \
  --create-namespace
```

From a local checkout:

```bash
helm install grid-operator charts/grid-operator \
  --namespace grid-system \
  --create-namespace
```

## Versioning

The chart follows [Semantic Versioning](https://semver.org/). In
`Chart.yaml`, `version` identifies the Helm chart package and `appVersion`
identifies the default `ghcr.io/praxis-proxy/grid-operator` image. The two
versions may advance independently, but Grid releases keep them aligned when
the chart and operator ship together.

## Verify

```bash
kubectl api-resources --api-group=grid.praxis.fast
kubectl get deployment grid-operator -n grid-system
helm test grid-operator -n grid-system
```

## Uninstall

```bash
helm uninstall grid-operator -n grid-system
```

Helm removes the namespaced resources and, with `crds.keep: true` (the
default), keeps the CRDs and every custom resource. To remove CRDs and all
custom resources:

```bash
kubectl delete crd agenttoolproviders.grid.praxis.fast \
  gridnetworks.grid.praxis.fast gridsites.grid.praxis.fast \
  inferenceproviders.grid.praxis.fast
```

## Upgrade

```bash
helm upgrade grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <new-version> \
  --namespace grid-system
```

The chart offers no migration guarantees at this stage. Across the API group move
from `grid.praxis-proxy.io` to `grid.praxis.fast`, the supported path is a fresh
install: uninstall, delete the four `grid.praxis-proxy.io` CRDs, and install
again. An upgrade deletes the old CRDs, and every Grid resource under them,
unless the previous release installed them with the `helm.sh/resource-policy: keep`
annotation, which `crds.keep` set at that time.

### Grid SWIM and signals

- The Deployment uses the Recreate strategy, so an upgrade stops the old pod
  before the new one starts. Two pods would gossip two identities for one site.
- With a LoadBalancer SWIM Service the operator waits for its address and never
  falls back to the Pod IP. `GRID_SWIM_ADVERTISE_ADDR` still carries the Pod IP
  for an older binary.
- The operator holds SWIM until the GridNetwork key loads. Set
  `swim.requireKey: false` to opt out.
- Peers learn each site's signals endpoint over SWIM, and dial port 9091 on a
  site that advertises none.
- The signals listener binds `[::]:9091`, or `0.0.0.0:9091` without IPv6.
- Unparseable `GRID_SIGNALS_*` settings fail at startup.

### Metrics over TLS

This chart version has no upgrade path from earlier ones; reinstall it. What changes:

- `metrics.tls.enabled: auto` serves `/metrics`, `/healthz`, and `/readyz` over HTTPS on
  OpenShift (service CA) and on any grid site (`grid.id` or `enrollment.enabled`, site
  identity). A scrape that still uses `http` fails. The chart's ServiceMonitor follows;
  update a hand-written one, or set `metrics.tls.enabled: false`.
- On OpenShift, `networkPolicy.enabled: auto` admits only the monitoring namespaces in
  `networkPolicy.metricsFrom` to the metrics and health port. Outside OpenShift no
  NetworkPolicy is rendered, so the port has no authentication; restrict it yourself or set
  `networkPolicy.enabled: true`.
- Under `siteIdentity` the port listens only after enrollment writes the identity, so the
  scrape target is down until then. Do not alert on it during enrollment.

### Gateway namespace

The operator looks for the gateway Service in the release namespace unless
`gateway.namespace` is set. Earlier charts left it to the binary default,
`grid-system`. If the release is outside `grid-system` and the gateway runs
there, set `gateway.namespace=grid-system` when you upgrade.

### CRDs

The CRDs are chart templates, so `helm upgrade` and an Argo CD sync upgrade
them. Set `crds.enabled: false` when a platform owns them, such as the RHOAI
`aiGrid` component, or for a second release in the same cluster.

Releases before this chart version installed the CRDs from `crds/`, so Helm
does not own them yet. First check that no other release owns them. The
release annotation must be empty or this release:

```bash
kubectl get crd agenttoolproviders.grid.praxis.fast gridnetworks.grid.praxis.fast \
  gridsites.grid.praxis.fast inferenceproviders.grid.praxis.fast \
  -o custom-columns='NAME:.metadata.name,RELEASE:.metadata.annotations.meta\.helm\.sh/release-name'
```

Then adopt them once. With Helm 3.17 or later:

```bash
helm upgrade grid-operator oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version <new-version> --namespace grid-system --take-ownership
```

With an older Helm, mark them as owned by the release, then upgrade as usual:

```bash
RELEASE=grid-operator; NAMESPACE=grid-system; for crd in agenttoolproviders gridnetworks gridsites inferenceproviders; do kubectl label crd "${crd}.grid.praxis.fast" app.kubernetes.io/managed-by=Helm --overwrite; kubectl annotate crd "${crd}.grid.praxis.fast" meta.helm.sh/release-name="${RELEASE}" meta.helm.sh/release-namespace="${NAMESPACE}" --overwrite; done
```

## Values

| Key | Type | Default | Description |
|-----|------|---------|-------------|
| `crds.enabled` | bool | `true` | Install and upgrade the Grid CRDs. `false` when a platform owns them. |
| `crds.keep` | bool | `true` | Keep the CRDs on `helm uninstall` and an Argo CD delete or prune. |
| `grid.providers` | object | `{}` | InferenceProviders this site serves, keyed by name. `model` defaults to the name, `providerKind` to `vllm`, `backendKind` to `local_model`. |
| `rbac.enrollmentNamespace` | string | `""` | The grid-enrollment namespace. The render fails if the operator would get Secret access there. |
| `rbac.metricsScraper` | bool | `true` | Create the metrics scraper ServiceAccount, allowed only GET on the nonResourceURL /metrics, and let the operator mint short-lived tokens for it. An llm-d EPP serving bearer-authenticated metrics (the default) admits a scrape with that token (metricsConfig.auth type serviceAccountToken). The operator never sends its own token. |
| `replicaCount` | int | `1` | Operator replicas. Must be 1 (schema-enforced). |
| `image.repository` | string | `ghcr.io/praxis-proxy/grid-operator` | Image repository. |
| `image.tag` | string | `""` | Image tag. Defaults to chart appVersion. |
| `image.digest` | string | `""` | Immutable digest. When set, tag is ignored. Must match `sha256:<64 hex>`. |
| `image.pullPolicy` | string | `IfNotPresent` | Image pull policy. |
| `imagePullSecrets` | list | `[]` | Pull secrets for private registries. |
| `nameOverride` | string | `""` | Override chart name in resource names. |
| `fullnameOverride` | string | `""` | Override fully qualified app name. |
| `commonLabels` | object | `{}` | Labels added to all resources. |
| `podLabels` | object | `{}` | Labels on the operator pod. |
| `podAnnotations` | object | `{}` | Annotations on the operator pod. |
| `serviceAccount.create` | bool | `true` | Create a ServiceAccount. |
| `serviceAccount.name` | string | `""` | ServiceAccount name. Defaults to fullname when `create` is true, `"default"` when false. |
| `serviceAccount.annotations` | object | `{}` | ServiceAccount annotations (e.g. IAM role binding). |
| `rbac.create` | bool | `true` | Create RBAC resources. |
| `resourceNamespaces` | list | `[]` | Additional namespaces for resource access. The release namespace is always included. |
| `log.level` | string | `info` | Level for every module: off, error, warn, info, debug, or trace, in any case. A full RUST_LOG directive still works here, as before. |
| `log.filter` | string | `""` | Full RUST_LOG directive, such as `info,operator=debug`. When set, it replaces `log.level`. |
| `metrics.bindAddress` | string | `0.0.0.0:9090` | Metrics server bind address. |
| `metrics.service.enabled` | bool | `true` | Create a metrics ClusterIP Service. |
| `metrics.service.port` | int | `9090` | Metrics Service port. |
| `metrics.service.annotations` | object | `{}` | Metrics Service annotations. |
| `metrics.tls.enabled` | bool or `auto` | `auto` | Serve `/metrics`, `/healthz`, and `/readyz` over TLS from `metrics.tls.source`. `false` serves plaintext. The probes switch to HTTPS, and the certificate reloads when it rotates. |
| `metrics.tls.source` | string | `auto` | `serviceCA`, `siteIdentity`, or `existingSecret`. `auto` picks `existingSecret` when set, else `serviceCA` on OpenShift, else `siteIdentity` when `grid.id` or `enrollment.enabled` is set. So every grid site serves HTTPS, and only an install with no grid identity serves plaintext. `siteIdentity` serves the enrolled `grid-site-identity` (`enrollment.identitySecretName`), read once enrollment writes it, so the port listens only after enrollment. An offline render without `--api-versions security.openshift.io/v1` picks `siteIdentity` rather than `serviceCA`. |
| `metrics.tls.existingSecret` | string | `""` | Secret holding `tls.crt` and `tls.key` for source `existingSecret`. For `serviceCA`, the metrics Service asks the OpenShift service CA for `<fullname>-metrics-tls`. |
| `metrics.tls.mountPath` | string | `/etc/grid/metrics-tls` | Mount path for the certificate and key. |
| `swim.bindAddress` | string | `0.0.0.0:7946` | SWIM protocol bind address. |
| `swim.advertiseAddress` | string | `""` | Externally reachable SWIM address. Defaults to the SWIM Service LoadBalancer address, else Pod IP. |
| `swim.requireKey` | bool | `true` | Hold SWIM traffic until the GridNetwork key loads or the network declares none. |
| `swim.siteName` | string | `""` | Bootstrap SWIM site name. |
| `swim.seeds` | string | `""` | Bootstrap SWIM seed endpoints (comma-separated `ip:port`, `[ipv6]:port`, or `hostname:port`). |
| `swim.service.enabled` | bool | `false` | Create a SWIM Service. |
| `swim.service.type` | string | `ClusterIP` | SWIM Service type. |
| `swim.service.port` | int | `7946` | SWIM Service port. |
| `swim.service.annotations` | object | `{}` | SWIM Service annotations. |
| `swim.service.loadBalancerIP` | string | `""` | Static IP for LoadBalancer. Deprecated in Kubernetes, so prefer `metallb.io/loadBalancerIPs`. |
| `swim.service.externalTrafficPolicy` | string | `""` | External traffic policy. Defaults to Local for LoadBalancer. |
| `swim.service.loadBalancerSourceRanges` | list | `[]` | Optional CIDRs allowed to reach the SWIM and signals LoadBalancer, where the implementation enforces them. |
| `signals.enabled` | bool | `false` | For signalTransport poll. Adds a TCP port named `signals` to the SWIM Service and points this site's gateway at it. Needs `swim.service.enabled`. A LoadBalancer must support mixed UDP and TCP ports. |
| `signals.port` | int | `9091` | Signals port on the SWIM Service. Peers learn the LoadBalancer address and this port over gossip. |
| `signals.advertiseAddress` | string | `""` | Signals endpoint gossiped to peers. Set it with `swim.advertiseAddress` or a NodePort Service, where the operator discovers no LoadBalancer address. |
| `gateway.address` | string | `""` | Advertised site gateway `host:port` override. Use it when the site gateway Service is not a LoadBalancer. Maps to `GRID_GATEWAY_ADDRESS`. |
| `gateway.serviceName` | string | `""` | Site gateway Service name the operator resolves and advertises to remote sites. Maps to `GRID_GATEWAY_SERVICE_NAME`. |
| `gateway.namespace` | string | `""` | Namespace of the site gateway Service. Empty uses the release namespace. Outside the resource namespaces, the operator gets only `get` on that one Service there. Maps to `GRID_GATEWAY_NAMESPACE`. |
| `gateway.allowSystemNamespace` | bool | `false` | Allow `gateway.namespace` to be `default`, `kube-*`, or `openshift-*`. |
| `gateway.port` | string | `""` | Site gateway Service port override. Empty uses the first Service `spec.ports` entry, with `8080` as the fallback if no usable port exists. Maps to `GRID_GATEWAY_PORT`. |
| `gateway.discoveryEnabled` | bool | `true` | Discover and advertise a LoadBalancer address for the site gateway. Maps to `GRID_GATEWAY_DISCOVERY_ENABLED`. |
| `health.liveness.initialDelaySeconds` | int | `5` | Liveness probe initial delay. |
| `health.liveness.periodSeconds` | int | `10` | Liveness probe period. |
| `health.startup.periodSeconds` | int | `10` | Startup probe period, for `metrics.tls.source` `siteIdentity`. |
| `health.startup.failureThreshold` | int | `96` | Startup probe failures before a restart. The window outlasts the 15-minute enrollment deadline, so a restart never interrupts enrollment. |
| `health.readiness.initialDelaySeconds` | int | `5` | Readiness probe initial delay. |
| `health.readiness.periodSeconds` | int | `10` | Readiness probe period. |
| `serviceMonitor.enabled` | bool | `false` | Create a Prometheus ServiceMonitor. |
| `serviceMonitor.labels` | object | `{}` | Additional ServiceMonitor labels. |
| `serviceMonitor.namespace` | string | `""` | ServiceMonitor namespace override. |
| `serviceMonitor.interval` | string | `30s` | Prometheus scrape interval. Empty leaves the Prometheus default. |
| `serviceMonitor.scrapeTimeout` | string | `10s` | Prometheus scrape timeout. Empty leaves the Prometheus default. |
| `networkPolicy.enabled` | bool or `auto` | `auto` | Render a NetworkPolicy for the operator pod. `auto` turns it on where OpenShift runs. SWIM and signals stay open to every peer. |
| `networkPolicy.metricsFrom` | list | the OpenShift user-workload and platform monitoring namespaces | NetworkPolicyPeer entries allowed to reach the metrics and health port. Kubelet probes are unaffected on OpenShift. Must not be empty. |
| `serviceMonitor.tlsConfig` | object | `{}` | Scrape TLS settings when `metrics.tls` is on. Empty: the OpenShift service CA and the Service DNS name for `serviceCA`, or the grid CA Secret and `<site>.grid.internal` for `siteIdentity`. Required with `existingSecret`. |
| `resources` | object | `{}` | Container resource requests and limits. |
| `nodeSelector` | object | `{}` | Node selector for scheduling. |
| `affinity` | object | `{}` | Pod affinity rules. |
| `tolerations` | list | `[]` | Pod tolerations. |
| `topologySpreadConstraints` | list | `[]` | Topology spread constraints. |
| `priorityClassName` | string | `""` | Pod priority class. |
| `enrollment.enabled` | bool | `false` | Enroll on startup when the site identity Secret is absent. |
| `enrollment.url` | string | `""` | Enrollment service base URL (https). |
| `enrollment.siteName` | string | `""` | Site name the token pins, at most 51 characters. |
| `enrollment.caBundle` | object | `{configMap: "", secret: "", key: ca.crt}` | CA bundle that pins the enrollment server, from exactly one of `configMap` and `secret`. |
| `enrollment.gridCaBundle` | object | `{configMap: "", secret: "", key: ca.crt}` | Grid CA the returned CA must match, from at most one source. Defaults to `caBundle`. |
| `enrollment.tokenSecretRef` | object | `{name: "", key: token}` | Secret in the release namespace holding the one-time site token. |
| `enrollment.identitySecretName` | string | `grid-site-identity` | Secret the site identity is written to when no `GridNetwork` names one. Installing the operator alone enrolls the site; no `GridNetwork` is needed. |
| `enrollment.caSecretName` | string | `grid-ca` | Secret the grid CA is written to when no `GridNetwork` names one. |
| `enrollment.rotation.enabled` | bool | `true` | Rotate the site identity before it expires, presenting the current one. Off under `pin` peer trust whatever this says. Needs an enrollment URL: the default with `enrollment.enabled`, or `enrollment.url` set, as on a hub whose identity bootstrap issued. |

## Join a grid

`site` and `grid` describe the site and the grid it joins in a few values:

```yaml
site: {name: east, region: us-east, zone: us-east-1a}
grid: {id: lab, peerTrust: spiffe, signals: poll, seeds: ["198.51.100.10:7946"]}
enrollment: {enabled: true, url: https://enroll.example.com}
```

`grid.id` renders the GridNetwork, with site discovery on and its TLS Secrets pointed at
the site identity, the grid CA, and `grid.swimKeySecretName`, plus this site's GridSite.
`site.name` and `grid.seeds` default `swim.siteName` and `swim.seeds`, which win when set,
and `grid.signals: poll` serves signals on the SWIM Service. The CRs need the grid CRDs
first. Argo CD applies them a sync wave after the CRDs. Plain Helm cannot map them on the
first install, so set `grid.id` on an upgrade after it, or install the grid-site chart.
`grid.signals` and `grid.peerTrust` also set the modes the operator starts in before any
GridNetwork exists, with or without `grid.id`. Set them to match the grid's GridNetwork,
wherever it comes from, and the operator never restarts when that network appears.

When enrollment is enabled or `grid.id` is set, an empty
`gateway.serviceName` becomes `grid-gateway`. A consumer-only site without a
gateway to advertise should set `gateway.discoveryEnabled: false`. This stops
Service lookups; an explicit `gateway.address` still wins when discovery is off.
For provider discovery, the Service must be `LoadBalancer`. ClusterIP and
NodePort gateways need an explicit reachable `gateway.address`.

## Auto-enroll

With `enrollment.enabled`, the operator enrolls on startup when the site identity Secret is absent, and reports ready after it enrolls. No `GridNetwork` is needed: the token pins the grid. The operator writes to the Secrets a `GridNetwork`'s `spec.tls.siteSecretRef` and `caSecretRef` name when one exists, and otherwise to `enrollment.identitySecretName` and `enrollment.caSecretName`. Both must be in the release namespace. With `rbac.create=false`, grant the operator get, create, and patch on Secrets. [Site Enrollment](../../docs/installation/enrollment.md#enroll-a-site) covers the hub and site steps.

With `enrollment.rotation.enabled`, the default, the operator rotates the site identity when less than a third of its lifetime remains. It presents the current certificate to the enrollment service, writes the new certificate and key into the same Secret, and keeps the replaced certificate under `previous.crt` until it expires. Consumers reload the Secret without a restart. The gateway loads its upstream client certificate only at start, so after each rotation the operator rolls the gateway Deployment named by `gateway.serviceName`, setting the pod template annotation `grid.praxis.fast/site-identity-fingerprint` to the new leaf's fingerprint. Rotation pins the enrollment service to `enrollment.caBundle` when set, and otherwise to the grid CA the site already holds. An identity that expired cannot rotate: `GridNetwork` `status.identity` reports `IdentityExpired`, and the site re-enrolls. The operator rotates only under `spiffe` peer trust, and follows the trust its `GridNetwork` declares. Before a `GridNetwork` exists it trusts by SPIFFE ID and rotates. Under `pin`, which a `GridNetwork` gets when it omits `peerTrust`, it logs `rotation disabled` when it finds pin trust, leaves `status.identity.rotateAfter` empty, and the site re-enrolls and is re-pinned before `status.identity.notAfter`. Setting `enrollment.rotation.enabled=false` stops this site's rotation and gateway roll and keeps its current identity until it expires. [Turn rotation off](../../docs/installation/enrollment.md#turn-rotation-off) covers turning it off for the whole grid.

## RBAC and namespace access

The chart creates two ClusterRoles:

1. **CRD access** (`<release>-crd`): cluster-wide get/list/watch/patch on
   GridNetworks and InferenceProviders; get/list/watch/patch/create/update on
   GridSites; get/patch on all three status subresources.
2. **Resource access** (`<release>-resources`): get/create/patch on Secrets;
   get on Services; create/patch on Events (`events.k8s.io`);
   get/create/patch/update on ConfigMaps.

Resource access is bound via RoleBindings. The release namespace always gets
a RoleBinding. Additional namespaces are added through `resourceNamespaces`:

```bash
helm upgrade grid-operator charts/grid-operator \
  --set "resourceNamespaces={app-ns,data-ns}" \
  --namespace grid-system
```

The `default` namespace is **not** implicitly included. Users must list it
in `resourceNamespaces` if the operator needs access there.

## Security

The chart enforces OpenShift-compatible restricted security defaults:

- `runAsNonRoot: true` (no fixed UID, so OpenShift can assign one)
- `readOnlyRootFilesystem: true`
- `allowPrivilegeEscalation: false`
- All Linux capabilities dropped
- `seccompProfile.type: RuntimeDefault`

## Monitoring

Enable a Prometheus ServiceMonitor (requires the Prometheus Operator CRD):

```yaml
serviceMonitor:
  enabled: true
  interval: 30s
```

## SWIM Service

Expose the SWIM port for cross-cluster mesh connectivity:

```yaml
swim:
  service:
    enabled: true
    type: LoadBalancer
    annotations:
      metallb.io/loadBalancerIPs: "192.0.2.10"
```

With a LoadBalancer Service and no `swim.advertiseAddress`, the operator
advertises the Service's LoadBalancer address and port. It reports not ready
until the address appears, logging an error after 3 minutes, and exits when
the address later changes so the restarted pod advertises the new one. A
hostname resolves once at startup, so it needs a stable IP. An explicit
`swim.advertiseAddress` always wins. Set it for a NodePort Service, which
otherwise advertises the Pod IP, not routable from remote clusters. Pin the
LoadBalancer IP with an annotation such as `metallb.io/loadBalancerIPs`.
The SWIM LoadBalancer Service publishes not-ready addresses, because some
implementations, such as k3s servicelb, publish an address only for an endpoint.

Signals on a LoadBalancer require `externalTrafficPolicy: Local`, the chart
default. The listener caps handshakes per source address, and `Cluster` SNAT
gives many peers one source. Set `swim.service.loadBalancerSourceRanges` to the peers'
egress CIDRs where the implementation enforces it.

The chart creates a Service but does not configure cross-cluster networking,
DNS, or firewall rules. Those remain deployment-platform responsibilities.
