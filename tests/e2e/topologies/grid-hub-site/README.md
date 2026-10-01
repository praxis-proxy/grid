# grid-hub-site

End-to-end test of `examples/helm/hub-site`. Forge creates two Kind clusters, `hub` and `site`, on one cross-cluster network, each with MetalLB. The site also runs the vllm-vcr mock model in namespace `model`. The topology installs no grid component. `e2e-hub-site.sh` in `scripts` installs the grid with `helm` and the example values, in the order the example README gives, and asserts on it.

## Run

From `scripts`:

```console
bash e2e-hub-site.sh           # build, up, test both modes, tear down
bash e2e-hub-site.sh up        # build and load the images, create the clusters
bash e2e-hub-site.sh test      # install and assert on existing clusters
bash e2e-hub-site.sh down      # delete the clusters
```

`MODES=pin` or `MODES=spiffe` runs one peer trust mode. `SKIP_BUILD=1` uses prebuilt images named by `IMAGE_PREFIX` and `IMAGE_TAG`. `KEEP=1` keeps the clusters. The script header lists every variable.

The script pins each LoadBalancer address from the MetalLB pool, so it knows the enrollment URL and SWIM seeds before install. The site gateway dials the mock model at its ClusterIP. A selectorless `grid-enrollment` Service in the site's `grid-enrollment` namespace forwards to the hub enrollment LoadBalancer, so the site operator enrolls by the Service name on the enrollment certificate.

## What It Asserts

For each mode in `MODES`, after it resets the `grid` and `grid-enrollment` namespaces on both clusters:

- The hub operator enrolls from its own invite, the site operator from the invite the example `kubectl` pipeline copies with its site label, and each identity carries its SPIFFE ID.
- The hub GridSite for the site is Active, the site sees the hub over SWIM, the hub GridNetwork is Active with a connected site, and the operator renders the hub overlay with a remote candidate. The hub gateway routes on its static backends, not on that overlay.
- The hub consumer gateway serves a chat completion over its static site backend, with `x-grid-provider-site: site-a`.
- With the hub identity, the site gateway serves the same completion over mTLS. Without a client certificate the handshake fails with a TLS alert. `/health`, which the backend serves, a `../` traversal, an encoded traversal, and `/v1/modelsX` get a 404 with no provider header, and `DELETE` gets a 405.
- The site gateway refuses `rogue`, an identity enrolled from its own invite: 403 in pin mode, a TLS alert in spiffe mode.
- No container in either namespace restarted, and no grid container log, init and hook containers included, matches ` ERROR`, `panic`, or `fatal`. WARN lines print for review.

Logs land in `ARTIFACTS` (default `/tmp/grid-hub-site-e2e`). Poll signal transport is not covered.

## Rootless Podman

Forge reads the cross-cluster network subnet from Docker's `network inspect` output, which `podman-docker` does not reproduce. Put a `docker` wrapper on `PATH` that answers `network inspect` in Docker's shape, set `KIND_EXPERIMENTAL_PROVIDER=podman`, and create the clusters under systemd delegation:

```console
KIND_CREATE_PREFIX="systemd-run --scope --user -p Delegate=yes" bash e2e-hub-site.sh up
```

MetalLB addresses on a rootless Podman network are not reachable from the host. Run `test` inside the network namespace, with a kubeconfig that names each control plane by its container address:

```console
podman unshare --rootless-netns env KUBECONFIG=<internal kubeconfig> bash e2e-hub-site.sh test
```
