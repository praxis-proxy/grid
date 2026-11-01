# Operator-derived local backend routing

This focused Forge/Kind qualification exercises the deployed path:

```text
Grid CRs -> operator serving ConfigMap (dial.json) -> mounted embedded Grid Gateway -> local inference Service
```

It verifies that the operator derives dials from local `InferenceProvider.spec.endpoint` values without a hand-configured Praxis backend list, that the
published and mounted dial documents agree, and that attributed inference requests reach both providers. It then makes Provider B's inference Service
unavailable while keeping its independent Grid health-check endpoint healthy. The embedded Gateway must detect repeated connection failures, demote that
backend, route subsequent requests to Provider A, recover Provider B when its original endpoint returns, and route to a replacement endpoint after the
provider spec changes. After publishing and mounting the replacement dial, the test verifies that the replacement endpoint eventually serves an
attributed request. It does not prove that replacement bypasses the Gateway's normal passive-health cooldown or that no transient request failures occur.

This does not replace the llm-d qualification's cross-site signal, capacity, or EPP-metric tests. It also does not test administrative `existing_only`
drain semantics, in-flight streaming behavior during drain, ingress, cross-cluster networking, or product API compatibility with a released Praxis image.
Those behaviors remain covered by their respective qualifications or require separate work.

## Prepare and run

Prerequisites: Docker, Kind, `kubectl`, Helm, Rust, and the pinned `praxis-forge` installed using [`scripts/forge.sh`](../../../../scripts/forge.sh).
Build all three Grid images from the same checkout. Unique tags prevent accidental reuse; the xtask records the local image IDs and the runtime Pod imageIDs.

```console
SOURCE_TAG="$(git rev-parse --short HEAD)-$(date -u +%Y%m%dT%H%M%SZ)"
GRID_GIT_COMMIT="$(git rev-parse HEAD)"
GRID_GIT_TREE_STATE="$(if test -z "$(git status --porcelain)"; then echo clean; else echo dirty; fi)"
GRID_BUILD_DATE="$(date -u +%Y%m%d)"
BUILD_ARGS=(
  --build-arg "GRID_GIT_COMMIT=$GRID_GIT_COMMIT"
  --build-arg "GRID_GIT_TREE_STATE=$GRID_GIT_TREE_STATE"
  --build-arg "GRID_BUILD_DATE=$GRID_BUILD_DATE"
)

docker build "${BUILD_ARGS[@]}" -f deploy/operator/Containerfile \
  -t "grid-operator:operator-routing-$SOURCE_TAG" .
docker build "${BUILD_ARGS[@]}" -f deploy/gateway/Containerfile \
  -t "grid-gateway:operator-routing-$SOURCE_TAG" .
docker build "${BUILD_ARGS[@]}" -f crates/mock-providers/Containerfile \
  -t "grid-mock-providers:operator-routing-$SOURCE_TAG" .

CLIENT_IMAGE_REF="curlimages/curl@sha256:fcff5cf7a4b895da7bd2933c914938db2b05d2113fa0d6c55b6d29930408f661"
docker pull "$CLIENT_IMAGE_REF"
docker tag "$CLIENT_IMAGE_REF" curlimages/curl:8.10.1

export GRID_XTASK_OPERATOR_IMAGE="grid-operator:operator-routing-$SOURCE_TAG"
export GRID_XTASK_GRID_GATEWAY_IMAGE="grid-gateway:operator-routing-$SOURCE_TAG"
export GRID_XTASK_MOCK_PROVIDER_IMAGE="grid-mock-providers:operator-routing-$SOURCE_TAG"
export GRID_XTASK_IMAGE_PULL_POLICY=Never

EVIDENCE_DIR="$(pwd)/evidence/operator-routing-$(date -u +%Y%m%dT%H%M%SZ)"
cargo xtask env run-grid-operator-routing-qualification \
  --forge-config tests/e2e/topologies/grid-operator-routing/forge.yaml \
  --teardown --keep-on-failure --evidence-dir "$EVIDENCE_DIR"
```

The runner uses a unique Forge environment, Kind cluster prefix, and Forge state directory. It first creates the cluster with `forge up`, then imports the
three local images and the digest-pinned curl client, then applies `operator`, `mock-providers`, and `grid-resources` in dependency order. Before installing
the embedded Gateway, it creates a short-lived local Grid identity Secret using generated test certificates; the private files are discarded immediately
after the Secret is created and are not written to evidence. It then applies `embedded-gateway`. It saves the resolved config,
each stack log, and diagnostics under `EVIDENCE_DIR`. On success it tears down its Forge environment and verifies the run-owned Kind cluster is gone. On
failure it captures diagnostics first and, because `--keep-on-failure` is set, retains only that run's cluster and temporary resolved Forge config for
investigation. The resolved config path and state directory are recorded in `evidence.json`; use those exact values with the pinned Forge binary for manual
cleanup. Do not remove the shared Docker `kind` network or touch other runs' clusters.

## Proofs and evidence

The machine-readable `evidence.json` records named phase results and timestamps, the Grid HEAD and source fingerprint (including untracked source files),
the resolved Forge config, selected image references/local IDs, and ready Pod imageIDs. Probe observations retain the HTTP status and Grid/backend attribution.

A passing run requires all of the following:

- both current-generation `InferenceProvider` resources are `Available`;
- the operator publishes the expected derived dials and the mounted `dial.json` exactly matches the ConfigMap content;
- successful initial requests are attributed to both expected provider backends;
- Provider B returns an attributable connection-failure response while its Grid health check remains healthy;
- a new passive-health log entry confirms Provider B demotion, followed by ten attributed Provider A responses;
- Provider B's original endpoint returns and serves two successful, correctly attributed requests;
- changing Provider B's endpoint publishes and mounts the replacement address, and the replacement backend eventually serves a correctly attributed request;
- run-owned cluster teardown completes.

The mock backend response marker, `x-grid-test-provider`, is checked against Grid's `x-grid-site` and `x-grid-backend` response attribution. A 200 response
without a consistent mapping cannot satisfy the routing assertions.
