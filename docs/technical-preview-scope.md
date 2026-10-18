# Technical Preview Scope and Limitations

AI Grid Network (AGN) is a network-routing component for AI traffic.
It prepares provider eligibility and routing state for Praxis gateways. This
page explains what AGN does and what you need to provide to use it.
It describes responsibilities, not every feature available in a particular
build.

## What AGN Does

- AGN's routing scope is Layer 7 AI requests. Its operator discovers and
  evaluates providers and publishes routing configuration; Praxis gateways
  handle HTTP requests. AGN does not route arbitrary Layer 3 or Layer 4
  traffic, provide a cluster network, or proxy requests through the operator.
  See the [architecture overview](architecture/overview.md) and
  [routing guide](routing.md).
- AGN can select among configured, eligible inference providers across sites.
  The provider deployments, their credentials, and the networks connecting
  sites must already exist. See [adding a provider](adding-provider.md).

## What The Deployment Owner Supplies

| Responsibility | Scope outside AGN |
| --- | --- |
| Cluster and model lifecycle | Provision and operate Kubernetes clusters, inference backends, model deployments, and their capacity. AGN observes declared providers; it does not install models or manage their workloads. |
| Multi-cluster connectivity | Provide routable paths, DNS, firewall rules, and network policy for the documented control-plane and gateway-to-gateway connections. AGN's membership and mTLS mechanisms do not create an inter-cluster network. |
| Gateway and Secret operations | Supply compatible Praxis gateways and the required Secret objects. Configure the supported chart or operator mount lifecycle and any rollout or reload needed beyond the watched routing overlay. AGN does not copy credential Secrets between clusters. |
| External entry point | Operate the ingress controller, DNS, client-to-edge policy, and edge failover when needed. A Grid chart can render an OpenShift Route, but AGN does not provide the underlying ingress service. Provider selection begins after a request reaches an edge gateway. |

The [existing-cluster installation guide](installation/existing-clusters.md)
shows one Helm-based setup. For gateways in a different cluster from the
operator, the deployment owner must also deliver generated consumer
configuration across that boundary; the operator does not write to a remote
Kubernetes API server. See [cross-cluster limitations](architecture/operations.md#cross-cluster-limitations)
and the [external ingress contract](architecture/external-ingress.md).

## Consumer And Provider Roles

Separate consumer/edge gateways from provider gateways as distinct identities,
configurations, Services, and credential mounts. Dedicated clusters are
recommended when provider credentials, private backends, compliance ownership,
or failure and scaling budgets need an independent infrastructure boundary.
This is a topology recommendation, not a requirement that every role run in a
different cluster. A combined site can run both roles in one cluster with
separate gateway Deployments and policy; it shares a cluster control plane and
failure domain, so it does not provide the isolation of dedicated clusters.
Neither layout by itself supplies complete tenant isolation; the
deployment owner must provide the surrounding identity, authorization, Secret,
and network controls. See [auth and policy](architecture/auth.md) and
[deployment topologies](architecture/overview.md#deployment-topologies).

## Helm Charts

Grid separates the operator and gateway workloads into independent Helm
releases:

| Chart | Use | Boundary |
| --- | --- | --- |
| [`grid-operator`](../charts/grid-operator/README.md) | Deploys the Grid control-plane operator and, by default, its CRDs. With `grid.id`, it can also render the GridNetwork, this site's labelled GridSite, and InferenceProviders. Without `grid.id` or enrollment, the operator creates the GridNetwork and labelled GridSite once the site has an identity. | It reconciles Grid resources and references gateway Services; it does not create Praxis gateway Deployments. Set `crds.enabled: false` when a platform manages the CRDs. |
| [`praxis-gateway`](../charts/praxis-gateway/README.md) | Deploys a Praxis gateway as its own Deployment and Service; usable standalone or as a Grid consumer/provider gateway. | Install separate releases with distinct configuration and Services for separate gateway roles. The chart does not require the operator or Grid CRDs for standalone use. |
| [`grid-site` chart source](../charts/grid-site/Chart.yaml) | Alternative to the operator chart's `grid.*` values for rendering GridNetwork, GridSite, and InferenceProvider resources for a site. | The Grid CRDs must already be installed; this chart does not deploy the operator or gateway workloads. Use one rendering path for each set of resources. |
| [`grid-enrollment`](../charts/grid-enrollment/README.md) | Optionally deploys the enrollment service and its supporting resources. | Enrollment is a site-identity bootstrap path; it does not provision clusters, models, or gateways. |

Check each chart's version and upgrade guidance before rollout. The operator
chart does not promise a general migration path; its CRD API-group migration
requires a fresh install.

The charts package component workloads and Kubernetes resources; they do not
provision inter-cluster networking or provider models, integrate tenant/user
identity, or provide public ingress. The [existing-cluster installation guide](installation/existing-clusters.md)
shows a Helm-based setup and deployment-owner responsibilities.

## Adjacent Platform Capabilities

AGN is not a tenant portal, model catalog or deployment service, customer
identity provider, billing pipeline, or tenant observability UI. Those may
integrate with AGN and Praxis, but they are separate platform responsibilities.
AGN's routing and operational signals do not by themselves constitute an
authoritative usage-metering or billing record. See [operations](architecture/operations.md)
for the signals AGN does expose.
