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

`LLMD=1` replaces the recorded-response model with a small llm-d stack on each
cluster: two [llm-d-inference-sim](https://github.com/llm-d/llm-d-inference-sim)
pods (no GPU) behind the llm-d router (Envoy and the EPP). Each provider's
`metricsConfig` points at the EPP, so the operators poll real `llm_d_epp_*`
series and the hub gateway picks a site on them. It implies
`HUB_ROUTER=operator` and the poll transport. Pins are in
[tests/e2e/topologies/grid-hub-site-llmd](../../tests/e2e/topologies/grid-hub-site-llmd/README.md).

```bash
LLMD=1 ./scripts/e2e-hub-site.sh up
LLMD=1 MODES=spiffe ./scripts/e2e-hub-site.sh test
```

### Try it

Both providers read Ready once their EPP is scraped:

```bash
kubectl --context kind-grid-hub-site-llmd-hub -n grid get inferenceproviders
```

Fetch the hub's grid identity, then read the site's signals as a peer does:

```bash
hub=kind-grid-hub-site-llmd-hub site=kind-grid-hub-site-llmd-site
kubectl --context $hub -n grid get secret grid-ca -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/ca.crt
kubectl --context $hub -n grid get secret grid-site-identity -o jsonpath='{.data.tls\.crt}' | base64 -d > /tmp/hub.crt
kubectl --context $hub -n grid get secret grid-site-identity -o jsonpath='{.data.tls\.key}' | base64 -d > /tmp/hub.key
kubectl --context $site -n grid port-forward deployment/grid-operator 19091:9091 &
curl -s --cacert /tmp/ca.crt --cert /tmp/hub.crt --key /tmp/hub.key \
  --resolve site-a.grid.internal:19091:127.0.0.1 https://site-a.grid.internal:19091/v1/site/signals
```

Load site-a with eight streaming requests at a time, more than its two pods run
at once (`include_usage` lets the EPP time tokens):

```bash
gw=$(kubectl --context $site -n grid get svc grid-gateway -o jsonpath='{.status.loadBalancer.ingress[0].ip}')
body='{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"ping"}],"max_tokens":64,'
body+='"stream":true,"stream_options":{"include_usage":true}}'
end=$((SECONDS + 60))
while ((SECONDS < end)); do
  for _ in 1 2 3 4 5 6 7 8; do
    curl -s -o /dev/null --cacert /tmp/ca.crt --cert /tmp/hub.crt --key /tmp/hub.key \
      --resolve "site-a.grid.internal:8080:$gw" -H 'Content-Type: application/json' \
      -d "$body" https://site-a.grid.internal:8080/v1/chat/completions &
  done
  wait
done
```

Meanwhile, ask the hub 20 times and count which site answers. A site answer
carries `x-grid-provider-site`, and one the hub served itself carries only
`x-grid-site: hub`:

```bash
kubectl --context $hub -n grid port-forward service/grid-gateway 18080:8080 &
for _ in $(seq 20); do
  curl -s -o /dev/null -D - -H 'Content-Type: application/json' \
    -d '{"model":"Qwen/Qwen3-0.6B","messages":[{"role":"user","content":"ping"}],"max_tokens":8}' \
    http://127.0.0.1:18080/v1/chat/completions | grep -iE '^x-grid-(provider-)?site' | tr -d '\r'
done | sort | uniq -c
```

Idle, the sites split the answers. Under load the hub answers them all, because
site-a's in-flight and `grid_route_site_rho` rise. Its per-site weight stays at
8: the pick moves on rho and in-flight. The signals return to idle within a few
polls, as [polling-metrics.md](../architecture/polling-metrics.md) describes.

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
- Membership on `main` still uses SWIM. Declared membership with SWIM off is in
  progress.

## More

- [Forge tooling](forge.md)
- [Conventions](conventions.md)
- [Type design](type-design.md)
