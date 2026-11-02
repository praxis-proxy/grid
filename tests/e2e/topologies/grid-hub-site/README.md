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

`MODES=pin` or `MODES=spiffe` runs one peer trust mode. `SIGNED_STATE=enforce` adds the signed state checks. `SKIP_BUILD=1` uses prebuilt images named by `IMAGE_PREFIX` and `IMAGE_TAG`. `KEEP=1` keeps the clusters. The script header lists every variable.

On non-amd64 hosts, `up` pulls the amd64-only VCR image and loads it into the site cluster before applying the model workload. Kind nodes keep their native architecture; the container runtime VM needs amd64 emulation enabled for the VCR container.

`MODEL_GOSSIP_TEST=1 HUB_ROUTER=operator SIGNED_STATE=enforce MODES=pin` runs the focused signed discovery path through the embedded gateway. It installs a test-only, file-configured HTTP fixture in the site cluster: `/v1/models` can return one model, an authoritative empty list, all 256 maximum-length names, or a deterministic 503. Completion requests continue to the real vllm-vcr backend. For synthetic names only, the fixture maps the request to VCR's configured Qwen model and restores the requested name in the JSON response, so the test exercises every discovered route without requiring VCR to load 256 models. The configured fallback model is absent from discovery throughout the run.

The focused run verifies singleton inference and its signed expiry; withdrawal and recovery before the original deadline; failed polling without freshness renewal or fallback routes; gateway rejection at `expires_at - 30s` while the hub operator is fully stopped and its old routing ConfigMap remains unchanged; and the exact 256-model candidate set, expiry, and inference for every name at concurrency four. It then withdraws the whole set and verifies recovery to the singleton. The default discovery poll interval is 60 seconds, inventory TTL is 180 seconds, and the retained-config expiry check allows eight seconds after the 30-second safety boundary. A run can therefore take several minutes.

The scenario saves site status, routing and serving configurations, captured timestamps, gateway expiry/readiness responses, the expected full model list, singleton inference responses, and per-model inference responses under `$ARTIFACTS/<mode>/model-gossip` and `$ARTIFACTS/<mode>/inference`. Set `IMAGE_BUILD_PROFILE=debug` (the script default) to build debug images; `release` selects release builds. Container builds share the Cargo registry cache and keep separate target caches for the root workspace and gateway workspace. On an ARM host, the script pulls and loads the `linux/amd64` vllm-vcr image for the site Kind cluster by default; set `VCR_PLATFORM` to override it. The ordinary static routing and withdrawal suite runs when `MODEL_GOSSIP_TEST` is off.

The script pins each LoadBalancer address from the MetalLB pool, so it knows the enrollment URL and SWIM seeds before install. The site gateway dials the mock model at its ClusterIP. A selectorless `grid-enrollment` Service in the site's `grid-enrollment` namespace forwards to the hub enrollment LoadBalancer, so the site operator enrolls by the Service name on the enrollment certificate.

## What It Asserts

For each mode in `MODES`, after it resets the `grid` and `grid-enrollment` namespaces on both clusters:

- The hub operator enrolls from its own invite, the site operator from the invite the example `kubectl` pipeline copies with its site label, and each identity carries its SPIFFE ID.
- The hub GridSite for the site is Active, the site sees the hub over SWIM, the hub GridNetwork is Active with a connected site, and the operator renders the hub overlay with a remote candidate. The hub gateway routes on its static backends, not on that overlay.
- In the `local` leg, neither operator has a SWIM Service, and neither own GridSite gets a `gatewayEndpoint` written to its spec. The site GridSite reaches `Connected=True` using `gateway.address` pointed at its in-cluster ClusterIP gateway Service.
- The hub consumer gateway serves a chat completion over its static site backend, with `x-grid-provider-site: site-a`.
- The hub consumer gateway serves a chat completion through its configured site backend, with `x-grid-provider-site: site-a`.
- With the hub identity, the site gateway serves the same completion over mTLS. Without a client certificate the handshake fails with a TLS alert. `/health`, which the backend serves, a `../` traversal, an encoded traversal, and `/v1/modelsX` get a 404 with no provider header, and `DELETE` gets a 405.
- The site gateway refuses `rogue`, an identity enrolled from its own invite: 403 in pin mode, a TLS alert in spiffe mode.
- With `SIGNED_STATE=enforce`, both operators sign their state, so the remote candidate above came from verified state. Once the site's GridNetwork turns signed state off, the hub refuses the site's unsigned state. Certificate rotation is excluded: a renewed key is locked out under enforce.
- With `MODEL_GOSSIP_TEST=1`, the site discovers its served model, the hub renders it from verified signed inventory with its expiry, and an inference request reaches the site provider. This focused path replaces the static routing and withdrawal assertions for that run.
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
