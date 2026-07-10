# Grid Operations Walkthrough

Step-by-step guide to grid formation, site lifecycle,
and routing configuration.

## 1. Deploy the Grid Operator

Install CRDs and the operator Deployment. The operator
starts its controller manager. No SWIM runtime starts
until a `GridNetwork` resource exists.

```console
kubectl apply -f grid-crds.yaml
kubectl apply -f operator.yaml
```

The operator runs as a single binary with multiple
controllers (one per CRD type) in the same process.

## 2. Create a GridNetwork

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridNetwork
metadata:
  name: production
spec:
  seeds:
    - grid.cluster-b.example.com:7946
  gatewayRefs:
    - name: inference-gw
      namespace: praxis-system
  tls:
    caSecretRef:
      name: grid-ca
      namespace: praxis-system
    siteSecretRef:
      name: grid-site-cert
      namespace: praxis-system
```

The GridNetwork controller:
1. Generates a grid CA via `certs`
2. Generates this site's certificate (DNS SAN:
   `{site-name}.grid.internal`, dual EKU for mTLS)
3. Stores both in Kubernetes Secrets
4. Generates a SWIM encryption key
5. Starts the SWIM runtime with seed peers
6. Sets `status.phase: Initializing`

## 3. Sites Discover Each Other

When the SWIM runtime contacts a seed peer:

**Grid ID negotiation**:
- Neither site has a `gridId`: deterministic tie-break
  (lexicographic site name), winner generates UUID,
  other adopts it
- Remote has a `gridId`, local doesn't: local adopts it
- Both have the same `gridId`: normal join
- Both have different `gridIds`: connection rejected
  (separate grids)

The operator creates a `GridSite` resource for the
discovered peer.

`GridSite` status: `phase: Discovered`

## 4. mTLS Certificate Exchange

For each Discovered `GridSite`:
1. Local operator sends its public cert PEM via SWIM
   piggyback broadcast (foca `BroadcastHandler`)
2. Remote operator receives and stores the cert
3. Each side appends the other's cert to their trust
   bundle Secret

`GridSite` status: `phase: Connecting`

## 5. Connectivity Verification

The operator verifies three conditions:

| Condition | Check |
|-----------|-------|
| `SWIMReachable` | SWIM probes succeeding |
| `MTLSEstablished` | mTLS handshake passes |
| `DataPlaneHealthy` | HTTP ping through Praxis |

A `connectivityPolicy` field on `GridNetwork`
(default: `requireAll: true`) leaves room for future
tolerance (partial connectivity, ignoring site types).

## 6. Capability Negotiation

Before reaching Active, sites exchange capability
summaries via SWIM piggyback:
- Which provider types they offer (inference, tools,
  agents)
- Provider counts per type

The `GridSite` status `capabilities` field is updated.
At least one provider type must be available.

`GridSite` status: `phase: Active`

## 7. Register Providers

Users or auto-discovery create provider resources.
See the [CRDs doc](crds.md) for full specs.

Example — an API provider:
```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: InferenceProvider
metadata:
  name: anthropic-api
spec:
  gridNetworkRef: production
  providerKind: anthropic
  backendKind: api_provider
  endpoint: https://api.anthropic.com
  models:
    - name: claude-sonnet-4
  auth:
    strategy: api_key
    secretRef: { name: anthropic-key }
  accessPolicy:
    siteSelector: {}
```

Example — a local llm-d cluster:
```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: InferenceProvider
metadata:
  name: local-vllm
spec:
  gridNetworkRef: production
  providerKind: self_hosted
  backendKind: local
  endpoint: http://vllm-service.inference:8000
  models:
    - name: llama-3.2-8b
```

## 8. Routing Configuration

The `PraxisConfig` controller watches all Active
`GridSites` and Available providers. For each Gateway
referenced by the `GridNetwork`, it generates an
overlay `ConfigMap` containing:

- **Cluster definitions**: one per remote site (with
  mTLS: CA, client cert, SNI), one per API/cloud
  provider
- **Grid scoring filter config**: backend list with
  kinds, costs, weights, provider types
- **Auth injection config**: per-cluster credential
  strategy and Secret references

The overlay `ConfigMap` is mounted into the Praxis
Deployment. Praxis hot-reloads its configuration.

## 9. Workloads Consume Providers

Workloads send requests to the Praxis Gateway.
The gateway's grid scoring filter selects the optimal
backend. Praxis handles API translation and credential
injection transparently.

See [Auth & Policy](auth.md) for workload access
patterns and authentication strategies.

## Site Departure

**Graceful leave**: Operator sends SWIM leave message.
Peers remove the site from membership immediately.
`GridSite` deleted.

**Crash**: SWIM probe fails (direct + indirect).
Site enters suspect state (default 10s timeout).
If no refutation, declared dead. `GridSite` status:
`Active → Unreachable → Left`.

## Adding a New Site to an Existing Grid

1. Deploy the Grid Operator on the new cluster
2. Create a `GridNetwork` with any existing cluster
   as a seed
3. SWIM discovers the existing cluster, which shares
   the membership list of all other sites
4. The new site automatically discovers all grid
   members within seconds
5. mTLS exchange and capability negotiation proceed
   with each peer
6. Once Active, the new site's providers are visible
   to all other sites
