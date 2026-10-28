# Developing Grid

Run a two-site grid on a laptop with kind: a hub and a site cluster with SPIFFE
peer trust and polled signals. CI runs the same script on every pull request.

## Dependencies

- Linux with Docker (macOS is not verified: images are linux/amd64 and kind
  LoadBalancer addresses are not reachable from the host)
- [kind](https://kind.sigs.k8s.io) v0.32.0, kubectl, [Helm](https://helm.sh) v3.17.3, Git
- Rust from `rust-toolchain.toml`, used to install [Forge](forge.md)
- 4 vCPU, 16 GB memory, 20 GB disk

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
  [examples/helm/hub-site](../../examples/helm/hub-site/README.md).
- `grid.membership.source` picks where a site learns its members. `Swim`, the default,
  discovers them by SWIM gossip. `Local` means this site's configuration lists its peers
  as `GridSite` objects, polls their signals, and runs no SWIM. Both are supported peer
  options.

## More

- [Forge tooling](forge.md)
- [Conventions](conventions.md)
- [Type design](type-design.md)
