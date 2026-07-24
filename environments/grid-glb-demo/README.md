# Grid GLB Demo Environment

Multi-cluster Grid environment demonstrating external ingress with
GLB-style failover. Three Kind clusters simulate a realistic topology:
one edge-control plane running Praxis AI as the external entry point,
and two provider clusters hosting simulated inference backends exposed
via MetalLB LoadBalancer Services.

## Architecture

```
                   +------------------+
                   |  Client (curl)   |
                   +--------+---------+
                            | :8080
               +------------v------------+
               |   grid-edge-us-east     |
               |   (Praxis AI gateway)   |
               +------------+------------+
                            | overlay routing
            +---------------+---------------+
            v                               v
   +--------------+                +--------------+
   |provider-east |                |provider-west |
   |  (Kind)      |                |  (Kind)      |
   |  GridSite    |                |  GridSite    |
   |  Operator    |                |  Operator    |
   |  mock-provs  |                |  mock-provs  |
   |  LB :8080    |                |  LB :8080    |
   +--------------+                +--------------+
```

Each provider cluster exposes its mock-inference backend through a
MetalLB-backed LoadBalancer Service (`provider-gateway`) on port 8080.
The edge-control cluster's GridNetwork references these LoadBalancer
IPs as `clusterEndpoints`, enabling the Grid overlay to route inference
requests across the cross-cluster Docker network.

## Current Status

### Runnable Now

- Cluster creation with cross-cluster Docker networking
- Gateway API CRDs, MetalLB with auto-configured address pools
- Grid operator (CRDs + deployment) on all clusters
- Per-cluster Grid CRD resources (GridNetwork, GridSites, InferenceProviders)
- Mock inference backends (Deployment + ClusterIP Service) on provider clusters
- Provider gateway LoadBalancer Services exposed via MetalLB
- Config validation passes: `praxis-forge config validate`

### Blocked on Upstream Packaging

- **grid-overlay-sync image** does not yet exist (`sha-PLACEHOLDER`).
  The overlay-sync service requires a packaged watcher that reads the
  operator-generated ConfigMap and writes grid-config.json.
- **Praxis AI hot-reload image** does not yet exist (`sha-PLACEHOLDER`).
  The edge service requires a Praxis AI build with file-based Grid
  config hot-reload support.
- **Transport** between edge and providers is set to `plaintext` for
  initial development. Production requires `mutual_tls` with proper
  SNI and certificate references.

## Prerequisites

- Docker (required for cross-cluster networking)
- [kind](https://kind.sigs.k8s.io/) v0.20+
- `praxis-forge` binary (built from this repo: `cargo build -p forge`)
- `kubectl`

## Demo Workflow

### 1. Validate the environment config

```console
praxis-forge config validate --config environments/grid-glb-demo/forge.yaml
```

### 2. Create clusters and network

```console
praxis-forge up --config environments/grid-glb-demo/forge.yaml
```

This creates three Kind clusters with cross-cluster Docker networking.
The standalone edge services are not active in `forge.yaml` yet because
their images are not published.

### 3. Apply provider stacks

Apply stacks to provider clusters first so their LoadBalancer IPs are
available before configuring the edge:

```console
praxis-forge stack apply provider-east --config environments/grid-glb-demo/forge.yaml
praxis-forge stack apply provider-west --config environments/grid-glb-demo/forge.yaml
```

This installs Gateway API CRDs, MetalLB, the Grid operator, provider
Grid CRDs, mock-inference Deployments, and the `provider-gateway`
LoadBalancer Service on each provider cluster.

### 4. Discover provider gateway IPs

After MetalLB assigns addresses, retrieve the LoadBalancer IPs:

```console
kubectl --context kind-grid-glb-provider-east get svc -n grid-system provider-gateway \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'

kubectl --context kind-grid-glb-provider-west get svc -n grid-system provider-gateway \
  -o jsonpath='{.status.loadBalancer.ingress[0].ip}'
```

### 5. Wire GridNetwork endpoints

Edit `resources/gridnetwork.yaml` and replace the placeholder addresses
with the discovered IPs:

```yaml
clusterEndpoints:
  - cluster: provider-east
    address: "<east-ip>:8080"
    transport:
      mode: plaintext
  - cluster: provider-west
    address: "<west-ip>:8080"
    transport:
      mode: plaintext
```

### 6. Apply edge-control stacks

```console
praxis-forge stack apply edge-control --config environments/grid-glb-demo/forge.yaml
```

This installs the Grid operator, GridNetwork (with wired endpoints),
edge GridSite, and access policies on the edge-control cluster.

### 7. Verify cluster status

```console
praxis-forge status --config environments/grid-glb-demo/forge.yaml
```

All three clusters should show `phase=running, live`.

### 8. Verify provider gateway reachability

From the Docker host, confirm the provider gateways respond:

```console
curl -s http://<east-ip>:8080/health
curl -s http://<west-ip>:8080/health
```

Both should return `ok` from the mock-inference health endpoint.

## End-to-End Demo (Blocked on Prerequisites Above)

Once the overlay-sync and hot-reload Praxis AI images are available:

### Send a test request through the edge

```console
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d @environments/grid-glb-demo/fixtures/requests/shared-model.json
```

### Confirm identity proof

```console
praxis-forge status --json --config environments/grid-glb-demo/forge.yaml \
  | jq '.data.services[] | select(.name == "grid-edge-us-east") | {containerId, startedAt, restartCount}'
```

`restartCount` should be `0` and `containerId` unchanged across the
failover window.

## Automated Proof (Not Yet Implemented)

The GLB ingress verifier will assert:

1. The edge service `containerId` remains stable across a simulated
   provider failover (provider-east cycled while provider-west absorbs
   traffic).
2. `restartCount == 0` for the edge service throughout the proof window.
3. Inference requests continue to succeed during failover (routed to the
   surviving provider via Grid overlay).

## Teardown

```console
praxis-forge down --config environments/grid-glb-demo/forge.yaml
```
