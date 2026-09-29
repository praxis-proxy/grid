# AI Grid Network Deployment Manifests

This directory contains deployment manifests for the `grid-operator` workload.

## Scope

This directory installs `grid-operator` and its CRDs.
It does **not** install or manage:

- Kind clusters or multi-cluster orchestration
- Praxis AI gateways or gateway configuration
- llm-d, mock EPP, or inference simulation
- MetalLB, Gateway API CRDs, or cross-cluster DNS
- Cross-cluster networking or service discovery

Multi-cluster development environment composition is
planned under the [Forge](https://github.com/praxis-proxy/grid/issues/2)
direction as a separate `praxis-forge` CLI.

## Directory Structure

- `crds/` - Custom Resource Definitions (auto-generated)
- `operator/` - `grid-operator` deployment manifests
- `examples/` - Example resource configurations (also see `../config/samples/`)

## CRDs

Custom Resource Definitions are generated from the operator source code:

```bash
# Regenerate CRDs after schema changes (writes deploy/crds and charts/grid-operator/crds)
make generate-crds

# Check that the committed CRDs match the Rust types (runs in CI)
make crds-check

# Validate CRD syntax
kubectl --dry-run=server create -k deploy/crds/
```

**Important**: Do not hand-edit CRD files. They are generated from the Rust code.

## Operator Installation

### Prerequisites

1. Kubernetes cluster with admin access
2. kubectl configured for the target cluster

### Install Steps

```bash
# Full install: CRDs + operator
kubectl apply -k deploy/

# Or step-by-step:
kubectl apply -k deploy/crds/
kubectl apply -k deploy/operator/

# Verify operator is running
kubectl get pods -n grid-system
kubectl logs -n grid-system deployment/grid-operator
```

### RBAC Structure

The operator uses a split RBAC model:

- **Cluster-scoped**: CRD access via ClusterRole `grid-operator-crd`
- **Namespace-scoped**: Secret/ConfigMap access via ClusterRole `grid-operator-resources` bound to specific namespaces

By default, the operator can access Secrets and ConfigMaps in the `default` namespace. To grant access to additional namespaces:

```bash
kubectl create rolebinding grid-operator-resources \
  --clusterrole=grid-operator-resources \
  --serviceaccount=grid-system:grid-operator \
  --namespace=YOUR-NAMESPACE
```

## Image Configuration

The checked-in operator `Deployment` references the published project image,
pinned to an immutable digest:

- `ghcr.io/praxis-proxy/grid-operator@sha256:8c8271aa589fbd81e346b75ae580be9e8085c3b283b4e6a99e2b9adcea73e12d`

Release notes provide the immutable digest for each released version. Do not
use `latest` as an installation contract.

The local Kind validation path continues to use:

- `grid-operator:latest`

For Kind validation, the xtask harness builds and loads the image automatically.
For production, use a versioned release tag or immutable digest.

## Praxis AI Gateway Deployment

**Important**: AGN only deploys the operator and CRDs. Praxis AI gateway deployment is separate and requires:

1. Praxis AI image with required filters (`intelligent_route`, `credential_inject`)
2. Consumer gateway configuration referencing AGN-generated ConfigMaps
3. Provider gateway deployment with AGN-compatible endpoints

## Container Images

The `grid-operator` image is built with a multi-stage Containerfile:

- **Build stage**: `rust:1.96-alpine` copies the whole workspace (`COPY . .`)
  and runs one `cargo build` for the operator, with BuildKit cache mounts on the
  Cargo registry and the target directory keeping incremental compiles fast.
- **Runtime stage**: `alpine:3.23` contains only CA certificates, a non-root
  `grid` user, and the operator binary.
- **Security**: multi-stage build, no build toolchain in the runtime image,
  non-root execution, and a restricted Kubernetes security context.

See `deploy/examples/` and `config/samples/` for complete deployment examples.

## Helm Chart

The recommended production installation path is the Helm chart at
[`charts/grid-operator/`](../charts/grid-operator/). The Kustomize manifests
in this directory are retained for development and raw-manifest users.

```bash
helm install grid-operator \
  oci://ghcr.io/praxis-proxy/charts/grid-operator \
  --version 0.1.1 \
  --namespace grid-system \
  --create-namespace
```

See the [chart README](../charts/grid-operator/README.md) for values,
upgrade procedures, and CRD lifecycle.

## Remaining Work

- **Forge dev environment**: multi-cluster orchestration is a separate future track (see [issue #2](https://github.com/praxis-proxy/grid/issues/2))
