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

- Every replica elects, and only the lease holder runs SWIM, so a rollout surges a
  candidate without gossiping a second identity for the site.
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

### Operator config

The chart renders `operatorConfig.name` only when the `enrollment` values give it a site
section. Another writer may own the ConfigMap otherwise; it must set the `config.yaml` key,
since a ConfigMap without it reads as an invalid config.

Upgrading from an earlier chart without `enrollment` values removes the ConfigMap from the
release, and Helm deletes it; `helm uninstall` does too. If another writer has written a site
section into it, keep it first:

```bash
kubectl -n grid-system annotate configmap grid-operator-config helm.sh/resource-policy=keep
```

A GitOps tool that applies the rendered manifests prunes it the same way unless the
ConfigMap tells it not to, for example Argo CD's `argocd.argoproj.io/sync-options: Prune=false`.

To set `enrollment` values while another writer owns the ConfigMap, set
`operatorConfig.create: false`. To hand it back to the chart instead:

1. Stop the other writer, or it rewrites `config.yaml` after the chart does.
2. Delete the ConfigMap, or give it the release's ownership metadata and drop the keep
   annotation. Otherwise the upgrade fails on an object the release does not own, and so
   does a rollback to an earlier chart, which always renders it:

```bash
kubectl -n grid-system label configmap grid-operator-config app.kubernetes.io/managed-by=Helm
kubectl -n grid-system annotate configmap grid-operator-config \
  meta.helm.sh/release-name=grid-operator meta.helm.sh/release-namespace=grid-system \
  helm.sh/resource-policy-
```

The chart's render then replaces its `config.yaml`.

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
| `replicaCount` | int | `1` | Operator replicas. Every replica is ready and elects: one holds the operator lease and writes, and the signals and SWIM Services route to it alone through an EndpointSlice it writes. An in-place upgrade from a release before leader election, where that release supports one (see the notes above), keeps the same replicaCount, then scales. |
| `podDisruptionBudget.enabled` | string or bool | `auto` | `auto` renders a PodDisruptionBudget when `replicaCount` is above 1; `true` or `false` forces it. |
| `podDisruptionBudget.minAvailable` | int or string | `1` | Pods that must stay up. Set this or `maxUnavailable`. The render fails for a budget that would block every node drain. |
| `podDisruptionBudget.maxUnavailable` | int or string | `""` | Pods that may be down at once. Set this or `minAvailable`: to use it, set `minAvailable` to null. |
| `shutdownDelaySeconds` | int | `null` | preStop sleep before the operator stands down. Empty: none. Above one replica the holder withdraws its own signals and SWIM endpoints, so only the metrics Service follows readiness, and a restart may miss one metrics scrape. |
| `terminationGracePeriodSeconds` | int | `null` | Empty: 30 plus `shutdownDelaySeconds` when there is a delay, else the Kubernetes default. Must cover 20 plus `shutdownDelaySeconds`. |
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
| `swim.seeds` | string | `""` | Bootstrap SWIM seed endpoints, separated by spaces or commas. A bare host or address gets `swim.service.port`; `host:port` and `[ipv6]:port` are taken as given. |
| `swim.service.enabled` | bool | `false` | Create a SWIM Service. |
| `swim.service.type` | string | `ClusterIP` | SWIM Service type. |
| `swim.service.port` | int | `7946` | SWIM Service port. |
| `swim.service.annotations` | object | `{}` | SWIM Service annotations. |
| `swim.service.loadBalancerIP` | string | `""` | Static IP for LoadBalancer. Deprecated in Kubernetes, so prefer `metallb.io/loadBalancerIPs`. |
| `swim.service.externalTrafficPolicy` | string | `""` | External traffic policy. Defaults to Local for LoadBalancer. |
| `swim.service.loadBalancerSourceRanges` | list | `[]` | Optional CIDRs allowed to reach the SWIM and signals LoadBalancer, where the implementation enforces them. |
| `platform` | string | `""` | Cloud whose load balancer settings LoadBalancer Services get: `aws` (an NLB, since the default carries no UDP, with cross-zone on the operator Services), `azure`, `gcp` (internal with global access), or `any` for all three. Each cloud reads only its own keys. |
| `loadBalancerScope` | string | `internal` | Reach of the SWIM and signals load balancers on a cloud platform. `internal` keeps them on the private network, so sites need private connectivity between them. `external` makes them internet-facing. AWS fixes the scope when it creates the load balancer, so changing it means recreating the Service. |
| `peers` | string | `""` | The grid's other sites, separated by spaces or commas. Each is a SWIM seed unless `grid.seeds` or `swim.seeds` is set, and each bare IPv4 address a host route in both Services' source ranges unless they set their own. That fits a peer that listens on the address it egresses from; on AWS with internet-facing load balancers, list the NAT addresses here and the SWIM Service hostnames in `grid.seeds`. Internal load balancers see peers' private addresses: leave this empty and set `swim.service.loadBalancerSourceRanges` to their machine networks. |
| `signals.enabled` | bool | `false` | For signalTransport poll. Adds a TCP port named `signals` to the SWIM Service and points this site's gateway at it. Needs `swim.service.enabled`. A LoadBalancer must support mixed UDP and TCP ports. |
| `signals.port` | int | `9091` | Signals port on the SWIM Service. Peers learn the LoadBalancer address and this port over gossip. |
| `signals.advertiseAddress` | string | `""` | Signals endpoint gossiped to peers. Set it with `swim.advertiseAddress` or a NodePort Service, where the operator discovers no LoadBalancer address. |
| `signals.route.enabled` | string or bool | `auto` | `auto` renders an OpenShift Route in front of the signals Service where the cluster serves `route.openshift.io/v1`; `true` or `false` forces it. An offline render without `--api-versions` renders `auto` as off. |
| `signals.route.host` | string | `""` | External hostname peers dial, the name the router matches the SNI against. The convention is `<site>-signals.<domain>`. Required when a Route renders. |
| `signals.route.annotations` | object | `{}` | Extra Route annotations. They override `rateLimit`'s on a key clash. |
| `signals.route.rateLimit.enabled` | bool | `true` | Render the router's per-source connection limits. Required when signals are exposed through a Route: without them the listener is open to anyone who can reach the router. |
| `signals.route.rateLimit.concurrentTcp` | int | `10` | Concurrent TCP connections per source IP, in the router. |
| `signals.route.rateLimit.rateTcp` | int | `20` | TCP connection rate per source IP, in the router's rate window. |
| `signals.peerIntervalSeconds` | int or string | `""` | Seconds between peer signal polls, 1 to 99999, as `GRID_SIGNALS_PEER_INTERVAL_SECS`. Every site polls every other alive site, so a grid of N sites makes N*(N-1) polls each interval. Empty keeps the operator's own default. |
| `gateway.address` | string | `""` | Advertised site gateway `host:port` override. Use it when the site gateway Service is not a LoadBalancer. Maps to `GRID_GATEWAY_ADDRESS`. |
| `gateway.serviceName` | string | `""` | Site gateway Service name the operator resolves and advertises to remote sites. Maps to `GRID_GATEWAY_SERVICE_NAME`. |
| `gateway.namespace` | string | `""` | Namespace of the site gateway Service. Empty uses the release namespace. Outside the resource namespaces, the operator gets only `get` on that one Service there. Maps to `GRID_GATEWAY_NAMESPACE`. |
| `gateway.allowSystemNamespace` | bool | `false` | Allow `gateway.namespace` to be `default`, `kube-*`, or `openshift-*`. |
| `gateway.port` | string | `""` | Site gateway Service port override. Empty uses the first Service `spec.ports` entry, with `8080` as the fallback if no usable port exists. Maps to `GRID_GATEWAY_PORT`. |
| `gateway.discoveryEnabled` | bool | `true` | Discover and advertise a LoadBalancer address for the site gateway. Maps to `GRID_GATEWAY_DISCOVERY_ENABLED`. |
| `health.liveness.initialDelaySeconds` | int | `5` | Liveness probe initial delay. |
| `health.liveness.periodSeconds` | int | `10` | Liveness probe period. |
| `health.startup.periodSeconds` | int | `10` | Startup probe period, for `metrics.tls.source` `siteIdentity`. |
| `health.startup.failureThreshold` | int | `96` | Startup probe failures before a restart. Enrollment has no retry deadline; a `siteIdentity` pod still enrolling restarts after about 16 minutes by default. |
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
| `operatorConfig.create` | bool | `true` | Render the ConfigMap holding this operator's own settings, when the `enrollment` values give it a site section. Without one nothing renders, the operator runs on defaults, and another writer may own the ConfigMap. Set `false` to manage it yourself under `operatorConfig.name`. |
| `operatorConfig.name` | string | `grid-operator-config` | ConfigMap name, in the release namespace. The operator watches it, so an edit applies without a restart. |
| `enrollment.enabled` | bool | `false` | This site enrolls: turns on the SWIM LoadBalancer Service, site-identity metrics TLS and the enrollment namespace guard, and renders the signer and the invite into the operator config's `site` section. |
| `enrollment.url` | string | `""` | Signer base URL (https), `site.signerEndpoint.url`. Required unless `enrollment.hub` is set. Set alone on a hub whose identity bootstrap issued, it renders a `site` section that renews only. |
| `enrollment.hub` | bool | `false` | The grid-enrollment release runs on this cluster, so `enrollment.url` defaults to its Service in `rbac.enrollmentNamespace`. |
| `enrollment.hubService` | string | `grid-enrollment` | With `hub` set and no `url`, the grid-enrollment release's full name: its Service and the in-cluster name its serving certificate carries. Must match that release. |
| `enrollment.hubPort` | int | `8443` | With `hub` set and no `url`, the grid-enrollment Service's port. Must match that release, whose Service port derives from its `listenAddr`. The `GRID_SIGNER_HOSTS` default follows it. |
| `enrollment.siteName` | string | `""` | Site name the token pins, at most 51 characters, and the GridSite's name: `site.name`. Defaults to `swim.siteName`. |
| `enrollment.caBundle` | object | `{secret: "", key: ca.crt}` | `site.signerEndpoint.caBundleRef`, a Secret, since anyone who can edit a ConfigMap could pin a signer of their own. Unset, Secret `grid-ca-bundle` when `enabled`. Pin the issuing intermediate or the signer's serving certificate, never a public root. |
| `enrollment.signerHosts` | list | `[]` | Signer hosts the operator config may name, each `host` or `host:port`, as `GRID_SIGNER_HOSTS`. Empty defaults to the host and port of `enrollment.url`. |
| `enrollment.gridCaBundle` | object | `{secret: "", key: ca.crt}` | `site.signerEndpoint.gridCaBundleRef`, the grid CA the returned CA must match, a Secret. Defaults to `caBundle`. |
| `enrollment.tokenSecretRef` | object | `{name: "", key: token}` | `site.inviteSecretRef`, the Secret in the release namespace holding the one-time site token. Name defaults to `grid-invite-<siteName>`. |
| `enrollment.rotation` | string | `""` | `site.signerEndpoint.rotation`: `Enabled` or `Disabled`. Empty renders `Disabled` unless `grid.peerTrust` is `spiffe`. Under `spiffe` it is left out, and the operator's default is `Enabled`. |
| `enrollment.identitySecretName` | string | `grid-site-identity` | Identity Secret the rendered GridNetwork names. Without `grid.id` it must stay `grid-site-identity`. |
| `enrollment.caSecretName` | string | `grid-ca` | Grid CA Secret the rendered GridNetwork names. Without `grid.id` it must stay `grid-ca`. |

## Join a grid

`site` and `grid` describe the site and the grid it joins in a few values:

```yaml
site: {name: east, region: us-east, zone: us-east-1a}
grid: {id: lab, peerTrust: spiffe, signals: poll, seeds: ["198.51.100.10:7946"]}
enrollment: {enabled: true, url: https://enroll.example.com}
```

`grid.id` renders the GridNetwork, with site discovery on and its TLS Secrets pointed at
the site identity, the grid CA, and `grid.swimKeySecretName`, plus this site's GridSite,
labelled `grid.praxis.fast/provider-site`. This site's GridSite is declared like any
peer: by `grid.id`, the grid-site chart, GitOps or by hand.

Without `grid.id` or `enrollment.enabled`, the operator creates what nothing declares
once the site has an identity: its GridNetwork (`GRID_NETWORK`, default `grid`) and its
GridSite, named from the identity and labelled `grid.praxis.fast/provider-site`, each
once, only when missing. It never overwrites a record something else declared.
Enrollment does not wait for the GridSite. Nothing renews the identity until the operator config names a signer.
A release with `grid.id` or `enrollment.enabled` turns this off from its
first install, so Helm never meets an operator-created record. Without an identity or
`site.name` the operator idles, and a `site.name` that disagrees with the identity stops it.

Enabling `grid.id` or `enrollment.enabled` on a release that ran without either needs one
step first. The operator already created the GridNetwork, and it carries its
`grid.praxis.fast/created-by` label rather than Helm's ownership metadata. Turning the
operator's creation off does not transfer it, so the upgrade that starts rendering it
fails on ownership. Adopt it before that, naming the release and the namespace it installs
into:

```bash
kubectl label gridnetwork/<grid.network> app.kubernetes.io/managed-by=Helm --overwrite
kubectl annotate gridnetwork/<grid.network> \
  meta.helm.sh/release-name=<release> \
  meta.helm.sh/release-namespace=<namespace> --overwrite
```

The chart's upgrade test cannot cover this, because it pauses the operator and so never has
an operator-created record to adopt.

`site.name` and `grid.seeds` default `swim.siteName` and `swim.seeds`, which win when set,
and `grid.signals: poll` serves signals on the SWIM Service. `grid.seeds` also takes one
string separated by spaces or commas, as `swim.seeds` does. The CRs need the grid CRDs
first. Argo CD applies them a sync wave after the CRDs. Plain Helm cannot map them on the
first install, so set `grid.id` on an upgrade after it, or install the grid-site chart.
`grid.signals` and `grid.peerTrust` also set the modes the operator starts in before any
GridNetwork exists, with or without `grid.id`. Set them to match the grid's GridNetwork,
wherever it comes from, and the operator never restarts when that network appears.
`grid.signedState: enforce` signs state on the rendered GridNetwork and requires
`grid.id`; set it at every site together. Without `grid.id`, set
`peerTrust.signedState: enforce` on the GridNetwork you manage. See
[Signed Gossip Trust](../../docs/architecture/signed-gossip-trust.md).

When enrollment is enabled or `grid.id` is set, an empty
`gateway.serviceName` becomes `grid-gateway`. A consumer-only site without a
gateway to advertise should set `gateway.discoveryEnabled: false`. This stops
Service lookups; an explicit `gateway.address` still wins when discovery is off.
For provider discovery, the Service must be `LoadBalancer`. ClusterIP and
NodePort gateways need an explicit reachable `gateway.address`.

## Enrollment

The operator reads its own settings from the ConfigMap `operatorConfig.name` in its namespace, key `config.yaml`, through the API, and watches it, so an edit applies without a restart. The chart renders it from the `enrollment` values. Every section is optional. An unknown key inside a section refuses the whole document, and an unknown top-level section is ignored with a Warning Event, so an older operator keeps running on a newer document. An optional `version: 1` names the document. The `site` section names this site, the signer it enrolls and renews through, and the invite:

```yaml
site:
  name: site-a
  signerEndpoint:
    url: https://enroll.grid.example.com:8443
    caBundleRef: {kind: Secret, name: grid-ca-bundle}
    rotation: Disabled
  inviteSecretRef:
    name: grid-invite-site-a
```

Credentials never go in the ConfigMap. It names Secrets, which keep their labels: a bundle carries `grid.praxis.fast/signer-bundle: "true"`, and the invite carries `grid.praxis.fast/site: <site.name>`. Grid-wide settings stay on the GridNetwork.

While it has no identity, the operator enrolls at startup, every 15 seconds while it waits, and at once on an edit. This site is the one named `site.name`, and after enrollment the one the identity names. The GridNetwork's `Enrolled` condition reports the result, and its `Available` rolls this site's conditions up. A refused document reports `Accepted` `False` with `OperatorConfigInvalid` and the field at fault, and a `site.name` other than the identity's reports `SiteNameMismatch`. Until a `GridNetwork` exists nothing is sent, and the operator logs why and records an `AwaitingGridNetwork` Event on the ConfigMap, once. The grid-site chart renders one before enrollment, and `spec.gridId` names the grid, set once. Beside a site section the operator mints no ID, so an empty `spec.gridId` holds the grid runtime and `Accepted` reads `False` with `GridIdUnset`. The operator writes to the Secrets a `GridNetwork`'s `spec.tls.siteSecretRef` and `caSecretRef` name when one exists, and otherwise to `grid-site-identity` and `grid-ca`, in the release namespace. An operator that enrolls while waiting exits 0 once and restarts under the identity. With `rbac.create=false`, grant the operator get, create, and patch on Secrets, and get, list, and watch on ConfigMaps. [Site Enrollment](../../docs/installation/enrollment.md#3-enroll-the-site) covers the hub and site steps.

While `site.signerEndpoint` names a signer with `rotation: Enabled`, the default, the operator rotates the site identity when less than a third of its lifetime remains. It reads the config each round, so a URL edit applies without re-enrolling, and its `RotationOnSchedule` condition reports whether the identity renews before it expires. It presents the current certificate to the signer, writes the new certificate and key into the same Secret, and keeps the replaced certificate under `previous.crt` until it expires. Consumers reload the Secret without a restart. An identity Secret that names a cert-manager Certificate is given 15 minutes past its due time to be replaced before `RotationOnSchedule` reads `False` with `RotationOverdue`. The gateway loads its upstream client certificate only at start, so after each rotation the operator rolls the gateway Deployment named by `gateway.serviceName`, setting the pod template annotation `grid.praxis.fast/site-identity-fingerprint` to the new leaf's fingerprint. Rotation trusts the signer through the grid CA the site already holds together with `caBundleRef`. The bundle must be a labelled Secret and the signer host must be in `GRID_SIGNER_HOSTS`, so an edit to the config cannot point renewal at a signer of its own. An identity that expired cannot rotate: `IdentityValid` reads `False` with `CertificateExpired`, and the site re-enrolls. The operator rotates only under `spiffe` peer trust, and follows the trust its `GridNetwork` declares. Under `pin`, which a `GridNetwork` gets when it omits `peerTrust`, the chart renders `rotation: Disabled`. `Enabled` there reports `Accepted` `False` with `RotationUnsupportedUnderPinTrust` and nothing renews, so the site re-enrolls and is re-pinned before the expiry the `IdentityValid` message names. Setting `rotation: Disabled` stops this site's rotation and gateway roll and keeps its current identity until it expires, unless another writer renews it. [Turn rotation off](../../docs/installation/enrollment.md#turn-rotation-off) covers turning it off for the whole grid.

## RBAC and namespace access

The chart creates two ClusterRoles:

1. **CRD access** (`<release>-crd`): cluster-wide get/list/watch/patch on
   GridNetworks and InferenceProviders; get/list/watch/patch/create/update on
   GridSites; get/patch on all three status subresources.
2. **Resource access** (`<release>-resources`): get/create/patch on Secrets;
   get on the named gateway, SWIM, and signals Services; create/patch on
   Events (`events.k8s.io`), get/list/watch/create/patch/update on
   ConfigMaps, and get/patch on the one Deployment named by
   `gateway.serviceName`, which a rotation rolls.

It grants no other Deployment access. A gateway that delegates its Secret
mounts to the operator renders a Role for its own Deployment from the
praxis-gateway chart (`mountReconciliation.rbac`), bound to this release's
ServiceAccount; see
[Deployment access](../../docs/architecture/operations.md#deployment-access).
A v0.2.0 release granted every Deployment in its resource namespaces instead,
so upgrade or re-grant delegated gateways before upgrading the operator.

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
