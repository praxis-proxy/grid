# Topology B: Combined Consumer and Provider Sites

Every cluster runs both a consumer gateway and a provider gateway as separate
Deployments with separate Services, TLS identities, and Secret mounts.

```text
east-a cluster                         east-b cluster
  AGN Operator                           AGN Operator
  consumer gateway (port 8080)           consumer gateway (port 8080)
  provider gateway (port 8443)           provider gateway (port 8443)
  inference backend                      inference backend

west-a cluster                         west-b cluster
  AGN Operator                           AGN Operator
  consumer gateway (port 8080)           consumer gateway (port 8080)
  provider gateway (port 8443)           provider gateway (port 8443)
  inference backend                      inference backend
```

## Security boundary

Provider credentials are mounted only in the provider gateway Deployment.
Consumer and provider gateways use separate TLS Secrets and separate
ConfigMaps. Colocation on the same cluster does not collapse the trust
boundary — Kubernetes RBAC and separate ServiceAccount mounts enforce
isolation.

## Values files

Each site has five values files:

| File | Helm chart | Description |
|------|-----------|-------------|
| `<site>-operator.yaml` | `grid-operator` | SWIM identity, seeds, provider gateway discovery |
| `<site>-grid-site.yaml` | `grid-site` | GridNetwork, GridSite, InferenceProvider CRs |
| `<site>-grid-mock-providers.yaml` | `grid-mock-providers` | Mock backends, Services, NetworkPolicy |
| `<site>-consumer-gateway.yaml` | `praxis-gateway` | Consumer role, overlay, port 8080 |
| `<site>-provider-gateway.yaml` | `praxis-gateway` | Provider role, credentials, port 8443 |

The operator discovers and advertises the provider gateway for remote
sites. GridNetwork gateway references identify the consumer gateway for
routing overlay delivery. AGN topology CRs and mock inference backends are
managed by their own Helm releases for lifecycle independence.

## Installation

Use the shared installer with `topology: combined-site` in your inventory:

```bash
../scripts/install.sh inventory.yaml
```

With per-site overrides (values files are applied after example defaults):

```bash
../scripts/install.sh inventory.yaml \
  --site-values east-a:operator:/path/to/operator-overrides.yaml \
  --site-values east-a:consumer:/path/to/consumer-overrides.yaml
```

Or install manually per site. **Order matters** — CRs and mock
backends must exist before the operator can produce the overlay
that the consumer mounts:

```bash
# 1. AGN Operator (installs CRDs)
helm upgrade --install grid-operator ../../../../charts/grid-operator \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system --create-namespace \
  --values values/east-a-operator.yaml

# 2. Mock inference backends (must be healthy before CRs trigger health checks)
helm upgrade --install grid-mock-providers ../../../../charts/grid-mock-providers \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system \
  --values values/east-a-grid-mock-providers.yaml

# 3. AGN site topology CRs
helm upgrade --install grid-site ../../../../charts/grid-site \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system \
  --values values/east-a-grid-site.yaml

# 4. Provider gateway
helm upgrade --install provider-gateway ../../../../charts/praxis-gateway \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system \
  --values values/east-a-provider-gateway.yaml

# 5. Wait for overlay ConfigMap
kubectl --context "$EAST_A_CONTEXT" -n grid-system \
  wait --for=jsonpath='{.metadata.name}' \
  configmap -l grid.praxis.fast/network --timeout=120s

# 6. Consumer gateway
helm upgrade --install consumer-gateway ../../../../charts/praxis-gateway \
  --kube-context "$EAST_A_CONTEXT" \
  --namespace grid-system \
  --values values/east-a-consumer-gateway.yaml
```

Repeat for each site.

See the [parent README](../README.md) for configuration layers, common
customizations, and troubleshooting.

## Compared to dedicated-edge topology

| Aspect | Dedicated edge | Combined site |
|--------|---------------|---------------|
| Clusters | 4 (2 consumer, 2 provider) | 4 (each runs both roles) |
| Credential boundary | Cluster boundary | Deployment/Secret boundary |
| Independent scaling | Consumer and provider scale separately | Share cluster resources |
| Upgrade isolation | Full blast-radius separation | Rolling upgrades affect both roles |

Choose combined-site when cluster count is constrained. Choose dedicated-edge
when you need full blast-radius isolation between consumer and provider roles.
