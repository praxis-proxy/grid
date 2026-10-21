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
# Regenerate CRDs after schema changes (writes deploy/crds and charts/grid-operator/templates/crds)
make generate-crds

# Check that the committed CRDs match the Rust types (runs in CI)
make crds-check

# Validate CRD syntax
kubectl --dry-run=server create -k deploy/crds/
```

**Important**: Do not hand-edit CRD files. They are generated from the Rust code.

## Operator Installation

The base install manifests are generated from `charts/grid-operator`; do not
hand-edit them. Each generated directory keeps the `values.yaml` that renders
it. Cloud-specific Kustomize overlays reuse a generated base and patch its
Services.

- `operator/` is the platform base: the image and the namespace set through `params.env`,
  metrics TLS from the enrolled site identity, and no platform assumptions. A platform
  operator vendors this directory, or a flavor of it, and sets `namespace` in `params.env`
  to place it in its own namespace.
- Generated `overlays/<flavor>/` directories use the base values plus the flavor's
  `values.yaml` as a delta, rendered against the API versions in its `api-versions` file.
  `overlays/openshift/` takes metrics TLS from the service CA, adds a NetworkPolicy that
  admits the platform's monitoring namespaces, and adds a ServiceMonitor for user workload
  monitoring, whose selector and TLS server name follow `namespace` in `params.env`.
- `overlays/openshift/` also sets `platform: any`: internal load balancers for the SWIM and
  signals Services on AWS (NLBs with cross-zone load balancing, since the default Classic
  Load Balancer rejects UDP), Azure and GCP (with global access, so other regions reach
  them). Each cloud reads only its own keys, so the same render serves every OpenShift
  install.
- The SWIM and signals Services are `LoadBalancer`, because the operator refuses any other
  type for the Service it advertises and peer sites reach both through it. A cluster with
  no LoadBalancer provider leaves the operator waiting for an address and never ready. The
  site gateway Service is named `grid-gateway`, the name the chart uses once a grid exists;
  the `grid-operator-resources` ClusterRole grants `get` on that name only.
- `overlays/kind/` is the standalone install for a kind cluster: plain metrics, no
  NetworkPolicy, its own `grid-system` Namespace, and the resources RoleBinding in the
  `default` namespace that the install qualification checks.

```bash
# Regenerate after a chart change (writes the base and overlays with values.yaml)
make generate-deploy

# Check generated manifests and rendered overlays (runs in CI)
make deploy-check
```

`deploy-check` requires Helm, yq v4, jq, and Kustomize. It verifies that the
generated directories match the chart, and that the OpenShift render requests
internal load balancers on AWS, Azure and GCP for the separate UDP SWIM and TCP signals
Services.

### Install Steps

The CRDs and the namespace are the shared prerequisites. The base and the OpenShift
overlay target the grid-system namespace without creating it, so a fresh cluster
needs it first.
The kind overlay creates its own.

```bash
kubectl apply -k deploy/crds/
kubectl create namespace grid-system --dry-run=client -o yaml | kubectl apply -f -
```

Then pick one install variant. The base is the portable path and the overlays are
flavors of it, so applying more than one to a cluster layers a flavor's settings over
the same operator resources.

```bash
# Portable, in its default namespace (a platform sets its own)
kubectl apply -k deploy/operator/ -n grid-system

# or OpenShift
kubectl apply -k deploy/overlays/openshift/ -n grid-system

# or kind, which brings its own namespace
kubectl apply -k deploy/overlays/kind/
```

To run a different image from a consumer kustomization that vendors the base, use the
images transformer in that kustomization's directory. Merging a second
grid-operator-parameters ConfigMap does nothing, because the base's replacement has
already run by the time the merge applies.

```bash
(cd path/to/consumer-kustomization && kustomize edit set image ghcr.io/praxis-proxy/grid-operator=ghcr.io/praxis-proxy/grid-operator:sha-<commit>)
```

A Deployment another installer created, the chart or the hand-maintained manifests
before they were generated, may carry a different selector. A selector is immutable, so
delete that Deployment first only when it differs from the target:

```bash
kubectl get deployment grid-operator -n grid-system \
  -o jsonpath='{.spec.selector.matchLabels}'
kubectl kustomize deploy/operator/ |
  yq 'select(.kind == "Deployment") | .spec.selector.matchLabels'
```

Matching selectors apply in place and need no delete.

# Verify operator is running
kubectl get pods -n grid-system
kubectl logs -n grid-system deployment/grid-operator
```

### RBAC Structure

The operator uses a split RBAC model:

- **Cluster-scoped**: CRD access via ClusterRole `grid-operator-crd`
- **Namespace-scoped**: Secret/ConfigMap access via ClusterRole `grid-operator-resources` bound in the operator namespace, plus every namespace listed under `resourceNamespaces` in the directory's `values.yaml`

These manifests grant no Deployment access; a gateway that delegates its
Secret mounts grants `get` and `patch` on its own Deployment, as described under
[Deployment access](../docs/architecture/operations.md#deployment-access).
To grant access to another namespace without regenerating:

```bash
kubectl create rolebinding grid-operator-resources \
  --clusterrole=grid-operator-resources \
  --serviceaccount=grid-system:grid-operator \
  --namespace=YOUR-NAMESPACE
```

## Image Configuration

The generated `Deployment` references the chart's release image, `ghcr.io/praxis-proxy/grid-operator`
at the chart `appVersion`. `deploy/operator/params.env` carries the same value under
`grid-operator-image`, and the kustomization replaces the Deployment image from it, so a
platform operator sets the image by rewriting that one key. Release notes provide the
immutable digest for each released version; pin it there or in `values.yaml` when an
installation contract needs one.

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
