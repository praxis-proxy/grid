# Developing Grid

Run a two-site grid on a laptop with kind: a hub and a site cluster with SPIFFE
peer trust and polled signals. CI runs the same script on every pull request.

## Dependencies

- Linux with Docker (macOS is not verified: images are linux/amd64 and kind
  LoadBalancer addresses are not reachable from the host)
- [kind](https://kind.sigs.k8s.io) v0.32.0, kubectl, [Helm](https://helm.sh) v3.17.3, Git
- Rust from `rust-toolchain.toml`, used to install [Forge](forge.md)
- 4 vCPU, 16 GB memory, 20 GB disk; 6 vCPU for the simulated llm-d stack below

## Bring it up

```bash
git clone https://github.com/praxis-proxy/grid && cd grid
./scripts/forge.sh install

# Use published images for this commit instead of building them
export SKIP_BUILD=1 IMAGE_PREFIX=ghcr.io/praxis-proxy/grid- IMAGE_TAG=sha-$(git rev-parse HEAD | cut -c1-7)
for i in operator gateway enrollment; do docker pull "$IMAGE_PREFIX$i:$IMAGE_TAG"; done

# Create the hub and site clusters
./scripts/e2e-hub-site.sh up

# Install with SPIFFE trust (MODES=spiffe) and polled signals with
# operator-generated routing (HUB_ROUTER=operator); leaves the grid running
MODES=spiffe HUB_ROUTER=operator ./scripts/e2e-hub-site.sh test
```

To build the images yourself, unset `SKIP_BUILD`, `IMAGE_PREFIX` and `IMAGE_TAG`.

## Run a simulated llm-d stack

The default topology serves the model from one recorded-response pod with
nothing to scrape, so the grid routes without load signals. `LLMD=1` gives each
cluster a small llm-d stack instead: two
[llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim) pods (no
GPU), the llm-d router (an Envoy proxy and the endpoint picker, EPP) in front of
them, and an InferencePool. Each site's provider points at its router, and its
`metricsConfig` at the EPP's metrics port, so the operators poll real
`llm_d_epp_*` series and the hub gateway picks a site on them. Image pins and
the render command are in
[tests/e2e/topologies/grid-hub-site-llmd](../../tests/e2e/topologies/grid-hub-site-llmd/README.md).

```bash
LLMD=1 ./scripts/e2e-hub-site.sh up
LLMD=1 MODES=spiffe ./scripts/e2e-hub-site.sh test
```

`LLMD=1` implies `HUB_ROUTER=operator` and the poll transport. What runs, per
cluster:

| Namespace | Workload | Role |
|---|---|---|
| `llmd` | `llm-d-inference-sim` (2 pods) | the engine: four requests at a time per pod, 200 ms to first token, 20 ms per token |
| `llmd` | `llmd-router-epp` (Envoy + EPP) | takes requests on 8081, picks a pod, serves `/metrics` on 9090 |
| `grid` | `grid-operator` | scrapes the EPP, serves `/v1/site/signals`, polls the peer |
| `grid` | `grid-gateway` | the hub: picks a site from the signals; the site: forwards to its router |
| `grid-enrollment` | `grid-enrollment` | hub only |

The hub has two candidates for the model: `hub-local`, its own router, and
`site-a`.

### Try it

Every step below reads state; only the load loop sends requests. Contexts are
`kind-grid-hub-site-llmd-hub` and `kind-grid-hub-site-llmd-site`.

Each cluster's provider is Ready, its EPP scraped:

```bash
kubectl --context kind-grid-hub-site-llmd-hub -n grid get inferenceproviders
kubectl --context kind-grid-hub-site-llmd-site -n grid get inferenceproviders
```

The hub operator's own `/metrics` carries one row per provider and site, its own
by scrape and the site's by peer poll. `grid_provider_ready_endpoints` is the
EPP's count of engine pods with fresh metrics, 2 per site:

```bash
kubectl --context kind-grid-hub-site-llmd-hub -n grid get secret grid-ca \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/grid-ca.crt
kubectl --context kind-grid-hub-site-llmd-hub -n grid port-forward deployment/grid-operator 19090:9090 &
curl -s --cacert /tmp/grid-ca.crt --resolve hub.grid.internal:19090:127.0.0.1 https://hub.grid.internal:19090/metrics \
  | grep -E '^grid_provider_(ready|ready_endpoints|in_flight_requests|ttft_p50_seconds)\{'
```

The same readings as the site serves them to a peer, in the wire format of
[signals.md](../architecture/signals.md). Peers present a grid site certificate,
so use the hub's:

```bash
kubectl --context kind-grid-hub-site-llmd-hub -n grid get secret grid-site-identity \
  -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/hub.crt
kubectl --context kind-grid-hub-site-llmd-hub -n grid get secret grid-site-identity \
  -o jsonpath='{.data.tls\.key}' | base64 -d > /tmp/hub.key
kubectl --context kind-grid-hub-site-llmd-site -n grid port-forward deployment/grid-operator 19091:9091 &
curl -s --cacert /tmp/grid-ca.crt --cert /tmp/hub.crt --key /tmp/hub.key \
  --resolve site-a.grid.internal:19091:127.0.0.1 https://site-a.grid.internal:19091/v1/site/signals
```

Now load one site. The site gateway takes mTLS from grid identities, so the loop
below presents the hub's. Eight streaming requests at a time against two pods
that run four each queues work at the engine; `include_usage` lets the EPP
record tokens and time per token:

```bash
SITE_GW=$(kubectl --context kind-grid-hub-site-llmd-site -n grid get svc grid-gateway \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
body='{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"ping"}],
  "max_tokens":64,"stream":true,"stream_options":{"include_usage":true}}'
end=$((SECONDS + 60))
while ((SECONDS < end)); do
  for _ in 1 2 3 4 5 6 7 8; do
    curl -s -o /dev/null --cacert /tmp/grid-ca.crt --cert /tmp/hub.crt --key /tmp/hub.key \
      --resolve "site-a.grid.internal:8080:$SITE_GW" -H 'Content-Type: application/json' \
      -d "$body" https://site-a.grid.internal:8080/v1/chat/completions &
  done
  wait
done
```

While it runs, the operator rows above move for `site-a` only:
`grid_provider_in_flight_requests` reads 8 and `grid_provider_ttft_p50_seconds`
appears once 20 requests completed in a 30 s window. The hub gateway reads the
same signals; its admin listener on the pod's loopback shows how it reads the
two sites:

```bash
kubectl --context kind-grid-hub-site-llmd-hub -n grid port-forward deployment/grid-gateway 9901:9901 &
curl -s http://127.0.0.1:9901/metrics | grep -E '^grid_route_(site_rho|site_in_flight|decisions_total)\{'
```

`grid_route_site_in_flight` for `site-a` reads 8 and its `grid_route_site_rho`,
the saturation the gateway routes on, climbs past 0.5 while `hub-local` stays at
0. To see the decisions themselves, send requests through the hub gateway and
count the site each answer names. A site answer carries `x-grid-provider-site`;
one the hub served itself carries only `x-grid-site: hub`:

```bash
kubectl --context kind-grid-hub-site-llmd-hub -n grid port-forward service/grid-gateway 18080:8080 &
for _ in $(seq 20); do
  curl -s -o /dev/null -D - -H 'Content-Type: application/json' \
    -d '{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"ping"}],"max_tokens":8}' \
    http://127.0.0.1:18080/v1/chat/completions | grep -iE '^x-grid-(provider-)?site' | tr -d '\r'
done | sort | uniq -c
```

Idle, the two sites split the answers. During the load every answer comes from
the hub. Once the loop ends the site's readings return within a few polls;
[polling-metrics.md](../architecture/polling-metrics.md) gives the timings.
Which series are required for routing and which are optional is in the same
document.

## Look around

```bash
kubectl --context kind-grid-hub-site-hub  -n grid get gridsites,gridnetworks,inferenceproviders
kubectl --context kind-grid-hub-site-site -n grid get gridsites
```

## Tear down

```bash
./scripts/e2e-hub-site.sh down
```

## Notes

- The Helm commands the script runs are in
  [examples/helm/hub-site](../../examples/helm/hub-site/README.md). Under
  `LLMD=1` the script adds each provider's `metricsConfig`; the values are in
  the topology README.
- `grid.membership.source` picks where a site learns its members. `Swim`, the default,
  discovers them by SWIM gossip. `Local` means this site's configuration lists its peers
  as `GridSite` objects, polls their signals, and runs no SWIM. Both are supported peer
  options.

## More

- [Forge tooling](forge.md)
- [Conventions](conventions.md)
- [Type design](type-design.md)
