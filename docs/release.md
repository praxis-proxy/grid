# Release Process

## Versioning

Grid uses [Semantic Versioning][semver]. The workspace version is defined in
`workspace.package.version` in the root `Cargo.toml`. Workspace crates inherit
that version.

Each Helm chart has its own `version`, which must match the Grid release. The
`appVersion` for Grid-owned workloads also matches the Grid tag. The
`praxis-gateway` chart is different: its `appVersion` identifies the default
Praxis AI image and may advance independently of Grid.

[semver]: https://semver.org/

## Release Artifacts

A Grid release publishes:

- `ghcr.io/praxis-proxy/grid-operator`;
- `ghcr.io/praxis-proxy/grid-mock-providers`;
- `ghcr.io/praxis-proxy/grid-overlay-sync`;
- the `grid-operator`, `grid-site`, `praxis-gateway`, and
  `grid-mock-providers` Helm charts; and
- a GitHub Release containing generated notes and immutable artifact digests.

Grid does not publish a Praxis AI image or an AI rollup. The release workflow
verifies the pinned official Praxis AI image used by the `praxis-gateway` chart,
including its digest and OCI provenance.

Optional Praxis AI filters are an explicit deployment dependency. Examples or
qualifications that require optional filters must document the required Cargo
features and require the caller to provide a compatible image. They must not
silently substitute a Grid-owned AI build.

## Pre-release Checklist

Before opening a release preparation pull request:

- [ ] Update the workspace version in `Cargo.toml` and regenerate `Cargo.lock`.
- [ ] Update every Helm chart `version`.
- [ ] Update Grid workload chart `appVersion` values to the Grid tag.
- [ ] Verify the `praxis-gateway` `appVersion` and default image match the
      intended official Praxis AI release.
- [ ] Update the release workflow's pinned AI tag, digest, and source revision
      when the default Praxis AI image changes.
- [ ] Run `cargo +nightly-2026-03-28 fmt --all -- --check`.
- [ ] Run `cargo clippy --workspace --all-targets -- -D warnings`.
- [ ] Run `make test`, `make doc`, and `make lint`.
- [ ] Run `git diff --check` and validate the release workflow with
      `actionlint`.
- [ ] Lint and render all Helm charts with their required values.
- [ ] Validate affected Forge topologies.
- [ ] Run the relevant integration qualifications when routing, overlay, or
      gateway compatibility changes.
- [ ] Confirm generated evidence and local build artifacts are not committed.
- [ ] Review pull request labels so generated release notes are useful.

The first-class integration qualifications are documented in
[the documentation index](README.md#integration-qualifications). They build
isolated Kind environments, exercise runtime behavior, record evidence, and
clean up their resources.

## Integration qualification checklist

Run the individual qualification commands below against fresh source builds
and fresh, uniquely tagged images. There is deliberately no aggregate release
qualification command. Run the commands sequentially because several use Kind
clusters and Docker networks whose names can otherwise collide.

| Area | Command | Topology/config path | Classification | Required when |
|---|---|---|---|---|
| Provider traffic selection and round-robin | `cargo xtask env run-grid-provider-traffic-qualification --forge-config tests/e2e/topologies/grid-provider-traffic/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"` | `tests/e2e/topologies/grid-provider-traffic/forge.yaml` | Release qualification | Routing, provider candidates, overlay selection policy, provider attribution, or gateway compatibility changes |
| Static weighted provider selection | `cargo xtask env run-grid-static-weighted-qualification --forge-config tests/e2e/topologies/grid-static-weighted/forge.yaml --quick --teardown --evidence-dir "$EVIDENCE_DIR"` | `tests/e2e/topologies/grid-static-weighted/forge.yaml` | Release qualification | `weightedRandom`, static placement, provider capacity, weighted overlay, or cross-site capacity propagation changes |
| Distributed token quota | `cargo xtask env run-grid-token-rate-limit-qualification --forge-config tests/e2e/topologies/grid-token-rate-limit/forge.yaml --image-tag "$IMAGE_TAG" --evidence-dir "$EVIDENCE_DIR"` | `tests/e2e/topologies/grid-token-rate-limit/forge.yaml` | Experimental integration qualification | Quota, identity, Basic Auth, Valkey, gateway image, or shared-consumer changes |
| Single-cluster multi-gateway | `cargo xtask env run-grid-single-cluster-multi-gateway-qualification --forge-config tests/e2e/topologies/grid-single-cluster-multi-gateway/forge.yaml --evidence-dir "$EVIDENCE_DIR"` | `tests/e2e/topologies/grid-single-cluster-multi-gateway/forge.yaml` | Integration qualification | Same-site gateway lifecycle, overlay delivery, provider selection, concurrency, or NetworkPolicy behavior changes |
| Combined-site lifecycle | `cargo xtask env run-grid-combined-site-demo --forge-config tests/e2e/topologies/grid-combined-site/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"` | `tests/e2e/topologies/grid-combined-site/forge.yaml` | Experimental lifecycle qualification | Combined-site routing, provider add/remove/re-add, session fallback, rollout, trust, or lifecycle changes |
| GLB | `cargo xtask env run-grid-glb-demo --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"` | `tests/e2e/topologies/grid-glb-demo/forge.yaml` | Experimental integration test | GLB, ingress, edge/provider boundary, mTLS, or network-boundary changes |
| Workload inference / no ingress | `cargo xtask env run-grid-glb-demo --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml --no-ingress --full --teardown --evidence-dir "$EVIDENCE_DIR"` | Reuses `tests/e2e/topologies/grid-glb-demo/forge.yaml`; documented by `tests/e2e/topologies/grid-workload-inference/README.md` | Experimental integration test | Workload inference, no-ingress routing, or cluster-local workload entry changes |
| llm-d pool metrics pressure and recovery | `cargo xtask env run-grid-llmd-pool-metrics-demo --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"` | `tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml` | Experimental integration qualification | EPP metrics, queue/KV-cache scoring, pressure transitions, recovery, or metrics mTLS changes |

Use the following one-line command stack after setting `IMAGE_TAG` and
`EVIDENCE_DIR` for each run. The quota qualification additionally requires a
Praxis AI image built with the optional features shown below; the other
topologies require the filters documented in their READMEs and may use the
official compatible AI image where applicable.

```console
cargo xtask env run-grid-provider-traffic-qualification --forge-config tests/e2e/topologies/grid-provider-traffic/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"
cargo xtask env run-grid-static-weighted-qualification --forge-config tests/e2e/topologies/grid-static-weighted/forge.yaml --quick --teardown --evidence-dir "$EVIDENCE_DIR"
cargo xtask env run-grid-token-rate-limit-qualification --forge-config tests/e2e/topologies/grid-token-rate-limit/forge.yaml --image-tag "$IMAGE_TAG" --evidence-dir "$EVIDENCE_DIR"
cargo xtask env run-grid-single-cluster-multi-gateway-qualification --forge-config tests/e2e/topologies/grid-single-cluster-multi-gateway/forge.yaml --evidence-dir "$EVIDENCE_DIR"
cargo xtask env run-grid-combined-site-demo --forge-config tests/e2e/topologies/grid-combined-site/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"
cargo xtask env run-grid-glb-demo --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"
cargo xtask env run-grid-glb-demo --forge-config tests/e2e/topologies/grid-glb-demo/forge.yaml --no-ingress --full --teardown --evidence-dir "$EVIDENCE_DIR"
cargo xtask env run-grid-llmd-pool-metrics-demo --forge-config tests/e2e/topologies/grid-llmd-pool-metrics/forge.yaml --full --teardown --evidence-dir "$EVIDENCE_DIR"
```

### Images and optional features

Each run must use exact image references that are materialized into the
resolved Forge configuration and loaded into Kind when `imagePullPolicy:
Never` is used. Record the image IDs or immutable digests in the evidence.
Typical local overrides are:

```console
export GRID_XTASK_GATEWAY_IMAGE=praxis-ai:$IMAGE_TAG
export GRID_XTASK_OPERATOR_IMAGE=grid-operator:$IMAGE_TAG
export GRID_XTASK_OVERLAY_SYNC_IMAGE=grid-overlay-sync:$IMAGE_TAG
export GRID_XTASK_MOCK_PROVIDER_IMAGE=grid-mock-providers:$IMAGE_TAG
export GRID_XTASK_VCR_IMAGE=ghcr.io/neuralmagic/vllm-vcr:vllm0.23
export GRID_XTASK_IMAGE_PULL_POLICY=Never
```

The distributed token-quota qualification requires Praxis AI built with:

```text
token-rate-limit-filter,praxis-filter/basic-auth-filter
```

The image label and runtime registrations must match the compiled feature
set. The standard official AI image may be suitable for provider selection and
other gateway paths, but a quota run must be **BLOCKED**, never marked PASS, if
the required filters are absent or its feature label is unavailable or
inaccurate. See the [provider traffic README](../tests/e2e/topologies/grid-provider-traffic/README.md), [quota README](../tests/e2e/topologies/grid-token-rate-limit/README.md), and the other topology READMEs for exact image preparation.

### Evidence, cleanup, and release gates

Create a new UTC-stamped evidence directory for every run, normally beneath
`evidence/`. Evidence from an older commit, source tree, image tag, or image
digest is not valid for the current release. Generated evidence must not be
committed.

Every command must be run with its teardown option and must clean only the
clusters, processes, port-forwards, pods, and Docker networks it owns. A
missing prerequisite, unavailable image, missing Forge binary, or failed
readiness/convergence check is **BLOCKED** or **FAIL**, never PASS. Preserve
non-2xx responses and first failures in the evidence; do not hide them with
retries or reinterpret them as successful routing.

`cargo xtask env validate-all` is a separate legacy/local validation suite. It
does not run the release qualification matrix above and must not be presented
as a substitute for these topology-specific commands.

Experimental classification means that a topology is outside the default
supported release surface; it does not permit weakening assertions or ignoring
failed results. When an experimental path is changed or claimed in release
notes, run it and report its evidence independently.

### Qualification details

| Area | Behavior proved | Images and overrides | Evidence, runtime, and cleanup |
|---|---|---|---|
| Provider traffic selection and round-robin | Grid publishes stable provider candidates and groups; Praxis AI accepts the overlay and returns trusted attribution while eligible providers receive round-robin traffic. | Official compatible Praxis AI gateway image, plus the locally built Grid operator, overlay-sync, mock-provider, and VCR images. Use the `GRID_XTASK_*_IMAGE` overrides above when validating unreleased Grid code. | Write to the run’s UTC-stamped `EVIDENCE_DIR`; the full run is typically several minutes. `--teardown` removes run-owned resources. |
| Distributed token quota | Basic Auth precedes admission; Alice’s sliding-window budget is shared across consumers; routing spans sites; concurrency, expiry, restart persistence, Valkey fail-closed behavior, recovery, and NetworkPolicy are exercised. | Praxis AI must be built with `token-rate-limit-filter,praxis-filter/basic-auth-filter`; use `--image-tag` and the exact feature-enabled local AI image, with local Grid operator/overlay-sync/VCR images as required by the README. | Record structured quota and routing evidence under `EVIDENCE_DIR`; runtime is variable and materially longer than a smoke test. The command cleans up on completion or failure; do not use `--keep` for release evidence. |
| Single-cluster multi-gateway | Two consumer gateways independently accept and serve the same three-provider overlay inside one Kubernetes cluster and one GridSite; attributed round-robin selection, provider withdrawal/restoration, consumer failure/recovery, concurrent traffic, and NetworkPolicy boundaries are exercised. | The fixed `grid-operator:single-cluster-qualification`, `grid-overlay-sync:single-cluster-qualification`, and `praxis-ai:single-cluster-qualification` references are local-development defaults. Release validation should set `GRID_XTASK_OPERATOR_IMAGE`, `GRID_XTASK_OVERLAY_SYNC_IMAGE`, and `GRID_XTASK_GATEWAY_IMAGE` to unique references; set `GRID_XTASK_VCR_IMAGE` as needed. All resolved references are loaded into Kind under `imagePullPolicy: Never`. | The command records timestamped structured evidence and performs automatic cleanup unless `--keep` is explicitly supplied. It does not claim multi-site SWIM, WAN behavior, or a globally shared round-robin cursor. |
| Combined-site lifecycle | Combined-site bootstrap, trusted round-robin, provider drain and restoration, secondary add/remove/re-add, session fallback, revision convergence, and lifecycle cleanup. | Official compatible Praxis AI image plus local Grid operator, overlay-sync, mock-provider, and VCR images through the overrides above. | Save lifecycle timelines and request attribution under `EVIDENCE_DIR`; a full run is typically on the order of tens of minutes. `--teardown` performs bounded cleanup of owned clusters, pods, processes, and networks. |
| GLB | Global load-balancing and network-boundary behavior, including provider attribution and the configured ingress path. | Official compatible Praxis AI image plus the topology’s required local Grid/operator/mock-provider/VCR images; use the listed overrides and `Never` pull policy for local images. | Save results under `EVIDENCE_DIR`; use `--quick` for bounded diagnostics or `--full` for qualification. `--teardown` removes only run-owned resources. |
| Workload inference / no ingress | Workload inference through the no-ingress path and its provider/network behavior. This reuses the GLB command with `--no-ingress`; it is not a separate invented CLI command. | Same image set and overrides as GLB, with any optional AI features required by that topology’s README. | Save no-ingress evidence under `EVIDENCE_DIR`; use `--quick` for diagnostics or `--full` for qualification. `--teardown` removes only run-owned resources. |
| llm-d pool metrics pressure and recovery | Pool-metrics observation, pressure-aware placement, availability during transitions, and recovery after metrics return below threshold. | Official compatible Praxis AI image plus local Grid operator/overlay-sync and the llm-d/EPP images required by its README; `--metrics-mtls` and `--kv-cache` are optional command flags when the topology enables them. | Save metric, overlay, reload, request, and recovery timelines under `EVIDENCE_DIR`; runtime is variable and may be long. `--teardown` performs bounded owned-resource cleanup. |

## Tagging A Release

Tags use `v<MAJOR>.<MINOR>.<PATCH>`, for example `v0.1.4`. Create the tag only
from the reviewed commit after its pull request and CI checks pass. Sign the tag
using the project's normal Git signing configuration:

```console
git tag -s v0.1.4 -m "Grid v0.1.4"
git push origin v0.1.4
```

Do not move or recreate a published release tag. Prepare a new patch release
for corrections.

## Automated Publication

Pushing a valid release tag triggers the **Release** workflow. The workflow:

1. verifies that the tag matches the workspace version;
2. reruns lint, tests, and documentation validation from the tagged source;
3. verifies the pinned official Praxis AI image and provenance;
4. builds and publishes immutable Grid container images with SBOM and
   provenance attestations;
5. validates, packages, and publishes all Helm charts; and
6. creates the GitHub Release with generated notes and immutable digests.

The workflow can also be dispatched for an existing immutable release tag. A
manual dispatch does not replace the requirement for a reviewed, signed tag.

## Release Notes

Grid uses [GitHub Releases][releases] for release notes. There is no committed
version-specific changelog. Add the user-facing summary, compatibility notes,
upgrade considerations, qualification results, and demonstration links to the
GitHub Release page.

[releases]: https://github.com/praxis-proxy/grid/releases

## Release Branches

Release branches are optional. Create one from a release tag only when a
supported line needs a backport. Use `release/v<MAJOR>.<MINOR>.x`, cherry-pick
the focused fix, and publish a new patch tag through the normal workflow.
