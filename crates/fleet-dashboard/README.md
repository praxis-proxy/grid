# Grid Fleet Dashboard

An optional, single-pod web UI for the **hub** cluster of an AI Grid hub-and-spoke
deployment. It draws every spoke on a dark world map, colors it green, yellow, or
red from that spoke's own Prometheus, and shows GPU, model, throughput, latency,
and queue detail per site.

The dashboard reads the same registry `ConfigMap` the hub EPP mounts
(`epp-clusters`, key `clusters.yaml`), so the map and the router always agree on
what the fleet is. It is read-only: it never writes to spokes and never talks to
the EPP.

## How it fits in Grid

- **Opt-in.** Nothing in Grid builds, deploys, or depends on the dashboard. It is
  a workspace crate like any other (so `cargo test --workspace` covers it), but
  its Helm chart lives in this directory, not under `charts/`, so Grid's release
  pipeline never packages or publishes it. You deploy it only by installing the
  chart below.
- **Runs anywhere in the hub cluster.** The pod has no node selector, affinity,
  host path, host network, or persistent volume; it is stateless, non-root, and
  read-only-rootfs, and it needs only the API server (a namespaced `Role` to
  `get/list/watch` `configmaps` and `secrets`) and outbound HTTPS to each site's
  metrics endpoint. It is hub-shaped by design: it follows the hub's registry
  `ConfigMap`, so it runs wherever that `ConfigMap` is.
- **Two languages.** The backend is this Rust crate (Axum, kube-rs, reqwest). The
  UI under `web/` is React 19 + Leaflet + Recharts, built by Vite and embedded
  into the binary by `build.rs`. A plain `cargo build` works without Node and
  serves a placeholder page; the container build and `make fleet-dashboard-web`
  embed the real UI.

## Prerequisites

| Where | Requirement |
|---|---|
| Workstation | `oc`, `helm` 3.12+, `podman`, `curl`, mikefarah `yq` v4 |
| Hub | OpenShift 4.14+ (Route and oauth-proxy) or any Kubernetes 1.27+ with `route.enabled=false`, `ingress.enabled=true`, `ingress.host=<your host>`, `auth.oauthProxy.enabled=false` |
| Each spoke | OpenShift with the default `openshift-monitoring` stack; the `thanos-querier` Route must be reachable from the hub pod |
| Each spoke, for full data | NVIDIA GPU operator with DCGM exporter, vLLM pods scraped by user-workload monitoring, EPP ServiceMonitor collected (see Troubleshooting for what happens when they are missing) |

## Install on the hub

```bash
oc config use-context <hub-context>
helm upgrade --install grid-fleet-dashboard crates/fleet-dashboard/charts/grid-fleet-dashboard \
  --namespace aigrid-fleet --create-namespace \
  --set hub.name=<hub-name> --set hub.region=<hub-region>
oc -n aigrid-fleet get route grid-fleet-dashboard -o jsonpath='https://{.spec.host}{"\n"}'
```

Open the URL, sign in through the OpenShift OAuth page, and you will see an empty
map until a site is registered. Access is granted to any user who can
`get configmaps` in `aigrid-fleet`
(`oc adm policy add-role-to-user view <user> -n aigrid-fleet` for read-only viewers).

To pull from a private registry, create a pull Secret in `aigrid-fleet` and pass
`--set imagePullSecrets[0].name=<secret>`.

## Register a site

One command per spoke; safe to re-run. Each spoke and hub kubeconfig needs to be
its own file, minified to the one context you want the script to use:

```bash
mkdir -p ~/.kube/aigrid-ds && chmod 700 ~/.kube/aigrid-ds
oc config view --minify --flatten --context=<context> > ~/.kube/aigrid-ds/<name>.kubeconfig && chmod 600 ~/.kube/aigrid-ds/<name>.kubeconfig
```

Run that once for the hub context and once per spoke context. The spoke
kubeconfig needs cluster-admin, because the script creates a `ClusterRoleBinding`
on the spoke.

```bash
crates/fleet-dashboard/hack/register-site.sh aigrid-ds-spoke1 \
  --spoke-kubeconfig ~/.kube/aigrid-ds/spoke1.kubeconfig \
  --hub-kubeconfig   ~/.kube/aigrid-ds/hub.kubeconfig \
  --region us-east-2 --display-name Ohio --dc aws-us-east-2
```

What it does:

1. On the spoke: namespace `aigrid-fleet-metrics`, ServiceAccount
   `fleet-metrics-reader`, a `ClusterRoleBinding` to the built-in
   `cluster-monitoring-view` ClusterRole, and a long-lived token Secret.
2. Verifies the token against `https://<thanos-querier route>/api/v1/query?query=up`.
3. On the hub: Secret `site-<name>` with the token and the spoke's CA, and an
   entry in the registry `ConfigMap` with the site's region and metrics URL.

`crates/fleet-dashboard/hack/unregister-site.sh <name>` reverses it.

## Values reference

| Key | Default | Meaning |
|---|---|---|
| `image.repository` / `image.tag` / `image.pullPolicy` | `ghcr.io/praxis-proxy/grid-fleet-dashboard` / chart `appVersion` / `IfNotPresent` | Dashboard image |
| `replicaCount` | `1` | Replicas (the collector is stateless; more than one only adds load on spokes) |
| `serviceAccount.name` | `""` (release fullname) | ServiceAccount to create |
| `registry.configMap` / `registry.key` | `epp-clusters` / `clusters.yaml` | Registry `ConfigMap` in the release namespace. Not created by the chart |
| `metrics.mode` | `perSite` | `perSite` queries each site's `metricsURL`; `central` queries one store and injects `{<clusterLabel>="<site>"}` into every selector |
| `metrics.centralURL` | `""` | Central Prometheus-compatible URL for `central` mode |
| `metrics.clusterLabel` | `cluster` | Label that identifies a site in the central store |
| `metrics.pollInterval` / `metrics.siteTimeout` | `15s` / `8s` | Poll cadence and per-site timeout |
| `metrics.centralTokenSecret.name` / `.key` | `""` / `token` | Secret holding the central store's bearer token. Delivered as env `FLEET_CENTRAL_TOKEN`; there is deliberately no flag, so the token never appears in `ps` |
| `metrics.centralCA.configMap` / `.key` | `""` / `ca.crt` | CA bundle `ConfigMap` for `central` mode, mounted at `/etc/fleet-ca` and passed as `--central-ca-file`; trusted in addition to the platform roots |
| `thresholds.gpuUtilWarn` / `queueWarn` / `latencyWarnMs` / `redAfterFailures` | `90` / `50` / `5000` / `2` | Health thresholds |
| `hub.name` / `hub.region` / `hub.lat` / `hub.lng` | empty | The hub glyph; hidden when `name` is empty; geocoded from `region` when coordinates are absent |
| `queries.<key>` | see `src/defaults.yaml` | PromQL per site. Edit for different exporters; an empty string disables a key |
| `auth.oauthProxy.enabled` | `true` | OpenShift oauth-proxy sidecar with a SAR on `get configmaps` in the release namespace |
| `auth.oauthProxy.image` | `registry.redhat.io/openshift4/ose-oauth-proxy-rhel9:v4.17` | Sidecar image |
| `auth.oauthProxy.cookieSecret` | `""` | Fixed session secret; empty reuses the existing Secret across upgrades or generates one |
| `route.enabled` / `route.host` | `true` / `""` | OpenShift Route (`reencrypt` with the proxy, `edge` without) |
| `ingress.enabled` / `className` / `host` / `annotations` / `tls` | `false` / `""` / `fleet.example.com` / `{}` / `[]` | Ingress for non-OpenShift clusters; targets the plain HTTP port |
| `serviceMonitor.enabled` / `interval` / `labels` | `false` / `30s` / `{}` | Scrape the dashboard's own `/metrics` |
| `demo.enabled` | `false` | Synthetic eight-site fleet, no spokes required |
| `resources` | requests `100m/128Mi`, limits `500m/512Mi` | Dashboard container resources |
| `imagePullSecrets`, `nodeSelector`, `tolerations`, `affinity` | empty | Scheduling |

Every flag is also an environment variable, `FLEET_<UPPER_SNAKE>`; run
`fleet-dashboard --help` for the full list.

## API

The SPA is the only intended client, but the JSON is stable:

| Path | Returns |
|---|---|
| `GET /api/v1/config` | Poll interval, version, thresholds, and the oauth-proxy user |
| `GET /api/v1/fleet` | The latest snapshot: every site with health and metrics, plus fleet totals; `503` until the first poll |
| `GET /api/v1/sites/{name}` | One site plus its recent history |
| `GET /api/v1/series?range=1h\|6h\|24h` | Fleet-wide series folded from every site |
| `GET /api/v1/stream` | Server-sent events: the current snapshot, then every new one |
| `GET /healthz`, `GET /readyz`, `GET /metrics` | Liveness, readiness (first poll done), Prometheus metrics |

`tests/fixtures/` holds the responses the original Go implementation produced for
the demo fleet; `tests/golden_json.rs` asserts this crate still produces them.

## Development

```bash
cargo run -p fleet-dashboard -- --demo           # synthetic fleet on http://localhost:8080
cargo run -p fleet-dashboard -- --kubeconfig ~/.kube/hub.kubeconfig --namespace aigrid-fleet
cargo test -p fleet-dashboard                    # unit tests and the golden API gate
make fleet-dashboard-image                       # container image (builds the UI too)

cd crates/fleet-dashboard/web && npm ci && npm run dev  # UI dev server, proxies /api to :8080
cd crates/fleet-dashboard/web && npm test               # UI tests

UPDATE_GOLDEN=1 crates/fleet-dashboard/hack/helm-template-test.sh   # after an intentional chart change
```

To run the binary with the real UI outside a container, `make fleet-dashboard-web`
builds the UI and stages it under `webui/` (ignored by git), where `build.rs`
embeds it on the next `cargo build`.

The published image is built for `linux/amd64` only, like Grid's other images; a
mixed-architecture cluster needs a manifest list or a `nodeSelector`.

## Troubleshooting

| Symptom | Cause and fix |
|---|---|
| Site is red with reason `metrics unreachable (N consecutive failures)` | The pod cannot reach `metricsURL`, or the token is rejected. Run the same query the collector runs, by hand, with the Secret's token: `TOKEN=$(oc -n aigrid-fleet get secret site-<name> -o jsonpath='{.data.token}' \| base64 -d)`, then `curl -K <(printf 'header = "Authorization: Bearer %s"\n' "$TOKEN") "https://<thanos-host>/api/v1/query?query=up"`. A 403 means the `ClusterRoleBinding` on the spoke is missing; a TLS error means `ca.crt` is missing from the Secret (re-run `register-site.sh`) |
| Site is red with reason `no ready inference endpoints` | The `readyEndpoints` query returned `0`. Check that the EPP is up on the spoke and that `queries.readyEndpoints` matches your endpoint-picker's metric |
| Site is yellow with reason `GPU utilization 93% >= 90%` | Expected under real load, not a fault. Raise `thresholds.gpuUtilWarn` if the default is too sensitive |
| Site is yellow with reason `no data for gpuUtil` | DCGM exporter absent. The collector falls back to `avg(vllm:gpu_cache_usage_perc)*100`; set `queries.gpuUtil` explicitly to silence the reason |
| Site is yellow with reason `no data for models` (also `rps`, `tokensPerSec`, `queueDepth`) | vLLM metrics are not in the spoke's Prometheus. Enable user-workload monitoring and make sure a ServiceMonitor or PodMonitor selects the vLLM pods |
| Site appears in the unplaced tray | Unknown `region` label. Use an AWS/GCP/Azure region code or pass `--lat`/`--lng` to `register-site.sh` |
| Route returns 403 after login | The user cannot `get configmaps` in `aigrid-fleet`. Grant `view` on the namespace |
| Route returns 502 | oauth-proxy is up but the dashboard is not ready. `oc -n aigrid-fleet logs deploy/grid-fleet-dashboard -c dashboard` |
| `/readyz` returns 503, logs show `waiting for registry cache sync` | The dashboard's ServiceAccount is missing the RBAC to read the registry `ConfigMap` (see `templates/rbac.yaml`), or the `ConfigMap` named in `registry.configMap` does not exist in the release namespace. Readiness means the first poll of a synced registry has completed |
| `helm upgrade` logs users out | Only happens when `auth.oauthProxy.cookieSecret` changed; leave it empty to keep the generated value |

The dashboard's own `/metrics` endpoint exposes `fleet_dashboard_site_reachable{site}`
(1/0 per site), `fleet_dashboard_query_failures_total{site,key}`, and
`fleet_dashboard_poll_duration_seconds`.
