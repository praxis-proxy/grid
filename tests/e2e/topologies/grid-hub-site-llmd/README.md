# grid-hub-site-llmd

The `grid-hub-site` topology with a simulated llm-d stack on each cluster instead of one vllm-vcr pod on the site. Forge creates the `hub` and `site` Kind clusters on one cross-cluster network, each with MetalLB, the Gateway API Inference Extension v1.6.2 CRDs, and in namespace `llmd`: two `llm-d-inference-sim` pods, the llm-d router (an Envoy sidecar and the endpoint picker, EPP) in front of them, and the `llmd-router` InferencePool the EPP serves. The topology installs no grid component. `scripts/e2e-hub-site.sh` under `LLMD=1` installs the grid with `helm`, points each site's provider at its router on port 8081 and its `metricsConfig` at the EPP's metrics port 9090 on the same Service, and asserts on it. Routing then runs on what the operators poll from the EPPs, per [polling-metrics.md](../../../docs/architecture/polling-metrics.md).

## Run

From the repository root:

```console
LLMD=1 ./scripts/e2e-hub-site.sh up                 # build and load the images, create the clusters
LLMD=1 MODES=spiffe ./scripts/e2e-hub-site.sh test  # install and assert on existing clusters
LLMD=1 ./scripts/e2e-hub-site.sh down               # delete the clusters
```

`LLMD=1` selects this topology and defaults `HUB_ROUTER` to `operator`, so the hub gateway serves the operator's generated `gridSiteRoute` config over polled signals and the hub has a provider of its own, `hub-local`, backed by the hub's router. Every other variable of `e2e-hub-site.sh` applies. [docs/developing](../../../docs/developing/README.md) walks through what to look at once it is up.

## Pins

| Component | Image or manifest |
|---|---|
| Engine | `ghcr.io/llm-d/llm-d-inference-sim:v1.0.0` |
| EPP | `ghcr.io/llm-d/llm-d-router-endpoint-picker:v1.0.0` |
| Proxy | `docker.io/envoyproxy/envoy:distroless-v1.33.2` |
| InferencePool CRD | GIE v1.6.2 `v1-manifests.yaml`, by sha256 in `forge.yaml` |

`resources/router.yaml` is rendered from the `llm-d-router-standalone` chart in the llm-d-router repository at tag `v1.0.0`, which is not published to a chart registry, with `router/values.yaml`. To re-render after a bump:

```console
git clone --depth 1 --branch v1.0.0 https://github.com/llm-d/llm-d-router
helm dependency update llm-d-router/config/charts/llm-d-router-standalone
helm template llmd-router llm-d-router/config/charts/llm-d-router-standalone -n llmd \
  -f tests/e2e/topologies/grid-hub-site-llmd/router/values.yaml \
  > tests/e2e/topologies/grid-hub-site-llmd/resources/router.yaml
```

`router/values.yaml` sizes the EPP and Envoy for the simulator only (250m and 100m CPU requests; the chart asks for 8 and 4 CPUs for a real pool), passes `--fairness-id-metric-label-limit=0` so the EPP's exposition stays under the operator's 1 MiB read, turns the EPP's metrics authentication off, adds the `inflight-load-producer` plugin so `llm_d_epp_inflight_requests` is published, and gives every container a restricted pod-security context. The simulator runs four requests at a time per pod, with a 200 ms time to first token and 20 ms per token, so a client loop of eight queues at the engine and the EPP's running and waiting averages move.

## What It Asserts

Everything `grid-hub-site` asserts on the `operator` leg, and in addition: the hub operator exports `grid_provider_ready 1` and `grid_provider_ready_endpoints 2` for its own site, scraped from the hub's EPP, and for `site-a`, polled from the site's operator. Load generation is not part of the assertions.
