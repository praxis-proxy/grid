# Grid Deployment Manifests

This directory contains deployment manifests for the Grid operator.

## Scope

This directory installs the Grid operator and CRDs.
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
- `operator/` - Grid operator deployment manifests
- `examples/` - Example resource configurations (also see `../config/samples/`)

## CRDs

Custom Resource Definitions are generated from the operator source code:

```bash
# Regenerate CRDs after schema changes
./scripts/generate-deployment-crds.sh

# Validate CRD syntax
kubectl --dry-run=server create -f deploy/crds/
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
kubectl apply -f deploy/crds/
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

The checked-in operator `Deployment` currently references the reserved project
image path:

- `ghcr.io/praxis-proxy/grid-operator:latest`

This is an unpublished placeholder until release images exist. Do not treat
`latest` as the supported production installation contract. For production,
patch the Deployment to a release tag or SHA tag once published.

The local Kind validation path continues to use:

- `grid-operator:latest`

For Kind validation, the xtask harness builds and loads the image automatically.
For production, use a versioned release tag or SHA tag when available.

## Praxis AI Gateway Deployment

**Important**: Grid only deploys the operator and CRDs. Praxis AI gateway deployment is separate and requires:

1. Praxis AI image with required filters (`grid_route`, `grid_credential_inject`)
2. Consumer gateway configuration referencing Grid-generated ConfigMaps
3. Provider gateway deployment with Grid-compatible endpoints

## Container Images

Grid operator image builds use a multi-stage Containerfile:

- **Build stage**: `rust:1.96-alpine` compiles the operator with dependency
  caching from workspace manifests and stub sources.
- **Runtime stage**: `alpine:3.23` contains only CA certificates, a non-root
  `grid` user, and the operator binary.
- **Security**: multi-stage build, no build toolchain in the runtime image,
  non-root execution, and a restricted Kubernetes security context.

See `deploy/examples/` and `config/samples/` for complete deployment examples.

## Remaining Blockers

- **Operator image publishing**: `ghcr.io/praxis-proxy/grid-operator` is reserved but not yet published
- **Praxis AI gateway packaging**: separate from Grid; requires upstream PRs to land
- **Helm/OLM**: deferred unless demand materializes; current Kustomize/YAML paths are sufficient
- **Forge dev environment**: multi-cluster orchestration is a separate future track (see [issue #2](https://github.com/praxis-proxy/grid/issues/2))
