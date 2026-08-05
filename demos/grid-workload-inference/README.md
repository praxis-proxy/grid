# Grid Workload-Inference Demo

A four-cluster disposable Kind environment that proves workload-originated
inference through Grid provider selection. Workloads submit requests from
inside consumer clusters through their local Praxis consumer gateway; no public
endpoint, GTM emulator, or external ingress is involved.

See the architecture overview's
[Deployment Topologies](../../docs/architecture/overview.md#deployment-topologies)
section for the general topology, security boundaries, and tradeoffs between
the dedicated clusters used here and combined consumer/provider sites.

## Architecture

```text
+----------------------------+  +----------------------------+
| east consumer cluster      |  | west consumer cluster      |
|                            |  |                            |
| workload Job               |  | workload Job               |
|      |                     |  |      |                     |
|      v                     |  |      v                     |
| Praxis consumer gateway    |  | Praxis consumer gateway    |
| Grid operator and overlay  |  | Grid operator and overlay  |
+-------------+--------------+  +--------------+-------------+
              |                                |
              +--------- Grid selection -------+
                               |
                    +----------+----------+
                    |                     |
                    v                     v
+----------------------------+  +----------------------------+
| east provider cluster      |  | west provider cluster      |
|                            |  |                            |
| Praxis provider gateway    |  | Praxis provider gateway    |
|      |                     |  |      |                     |
|      v                     |  |      v                     |
| private simulated          |  | private simulated          |
| inference endpoint         |  | inference endpoint         |
| Grid operator              |  | Grid operator              |
+----------------------------+  +----------------------------+
```

Each consumer cluster runs a Praxis consumer gateway that receives requests from
in-cluster workloads. The Grid Operator on each site discovers providers
through SWIM, renders a per-edge overlay, and Praxis hot-reloads the
routing configuration. Provider gateways authenticate consumer peers via
mTLS and replace credentials at the final hop.

## Configuration

This demo uses the same Forge configuration as the GLB demo at
`demos/grid-glb-demo/forge.yaml`. The `--no-ingress` flag instructs xtask
to strip the GTM emulator cluster at render time, producing a four-cluster
resolved config. The source `forge.yaml` is never modified.

Consumer gateways use the same `configs/edge/praxis.yaml` as the GLB edge
gateways. The `X-Grid-Demo-Edge-Gateway` header identifies each consumer
site.

## Quick Start

```bash
cargo xtask env run-grid-glb-demo --no-ingress --quick --teardown
```

See [e2e-demo-output.txt](e2e-demo-output.txt) for example narrated output
from a quick cold run.

## Prerequisites

- Rust stable 1.96+
- Docker or Podman
- kind
- `praxis-forge` binary in PATH or `target/release`
- Approximately 16 GB RAM for four Kind clusters

## Image Overrides

Workload mode defaults to registry images with `IfNotPresent` pull policy.
The operator requires v0.1.1+ for the health endpoints used by the Helm
chart's liveness and readiness probes.

| Component       | Default Image                                      | Min Version |
|-----------------|----------------------------------------------------| ----------- |
| Gateway         | `ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.1`      | v0.1.1      |
| Operator        | `ghcr.io/praxis-proxy/grid-operator:v0.1.1`        | v0.1.1      |
| Mock providers  | `ghcr.io/praxis-proxy/grid-mock-providers:v0.1.1`  | v0.1.1      |

Override with environment variables:

```bash
GRID_XTASK_GATEWAY_IMAGE=myregistry/gateway:dev \
GRID_XTASK_OPERATOR_IMAGE=myregistry/operator:dev \
GRID_XTASK_MOCK_PROVIDER_IMAGE=myregistry/mock:dev \
GRID_XTASK_IMAGE_PULL_POLICY=Always \
  cargo xtask env run-grid-glb-demo --no-ingress --quick --teardown
```

## Proof Points

| #  | Proof                                          | Quick | Full |
|----|------------------------------------------------|-------|------|
| 1  | Four clusters created and healthy               | yes   | yes  |
| 2  | SWIM membership across all four sites            | yes   | yes  |
| 3  | Grid operators converge overlay for each edge    | yes   | yes  |
| 4  | Three provider candidates discovered             | yes   | yes  |
| 5  | Overlay revision chain integrity                 | yes   | yes  |
| 6  | East workload request succeeds (in-cluster Job)  | yes   | yes  |
| 7  | West workload request succeeds (in-cluster Job)  | yes   | yes  |
| 8  | Local provider preference observed               | skip  | yes  |
| 9  | Remote provider fallback after drain              | skip  | yes  |
| 10 | Provider mTLS required                           | yes   | yes  |
| 11 | Peer authorization enforced                      | yes   | yes  |
| 12 | Credential replacement at final hop              | yes   | yes  |
| 13 | Response attribution present                     | yes   | yes  |
| 14 | NetworkPolicy isolates backends                  | yes   | yes  |
| 15 | Clean teardown of four clusters                  | yes   | yes  |

## Existing-Cluster Examples

For deploying on existing Kubernetes clusters (not disposable Kind), see
`examples/helm/existing-clusters/`.

## Limitations

- No external ingress or public endpoint; add a traffic manager separately.
- Kind networking does not represent production latency or failure modes.
- Simulated inference providers return canned responses.
