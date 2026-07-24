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

Two host services complete the data path:

- **grid-overlay-sync-us-east** watches the operator-generated
  ConfigMap on edge-control and writes `grid-config.json` to a
  shared runtime directory (`environments/grid-glb-demo/.forge/runtime/edge-us-east/`).
- **grid-edge-us-east** runs Praxis AI with file-based Grid config,
  reading from the same runtime directory. It listens on
  `127.0.0.1:8080` for local client requests.

## Current Status

### Runnable Now

- Cluster creation with cross-cluster Docker networking
- Gateway API CRDs, MetalLB with auto-configured address pools
- Grid operator (CRDs + deployment) on all clusters
- Per-cluster Grid CRD resources (GridNetwork, GridSites, InferenceProviders)
- Mock inference backends (Deployment + ClusterIP Service) on provider clusters
- Provider gateway LoadBalancer Services exposed via MetalLB
- Automatic capture of provider gateway IPs into Forge state
- Template-manifest rendering of GridNetwork with captured IPs
- Config validation passes with full service definitions

### Blocked on Upstream Packaging

- **grid-overlay-sync image** (`sha-PLACEHOLDER`): the overlay-sync
  service requires a packaged watcher that reads the
  operator-generated ConfigMap and writes `grid-config.json`. No
  published image exists yet.
- **Praxis AI hot-reload image** (`sha-PLACEHOLDER`): the edge
  service requires a Praxis AI build with file-based Grid config
  hot-reload support. No published image exists yet.
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

This creates three Kind clusters (`edge-control`, `provider-east`,
`provider-west`) with a shared Docker network (`grid-glb-demo-net`).
The host edge services are marked `autoStart: false` until their
placeholder images are replaced, so `up` will not try to start them.

### 3. Apply provider stacks

Apply stacks to provider clusters first. The `inference-sim` stack
waits for the `provider-gateway` LoadBalancer Service to receive an IP,
then captures it into Forge state automatically:

```console
praxis-forge stack apply provider-east --config environments/grid-glb-demo/forge.yaml
praxis-forge stack apply provider-west --config environments/grid-glb-demo/forge.yaml
```

This installs Gateway API CRDs, MetalLB, the Grid operator, provider
Grid CRDs, mock-inference Deployments, and the `provider-gateway`
LoadBalancer Service on each provider cluster. The captured IPs are
stored in `.forge/state.json` for use by downstream stacks.

### 4. Apply edge-control stacks

```console
praxis-forge stack apply edge-control --config environments/grid-glb-demo/forge.yaml
```

The `edge-demo` stack uses `template-manifest` to render
`gridnetwork.yaml` with the captured provider gateway IPs. No manual
YAML editing is required.

### 5. Verify cluster status

```console
praxis-forge status --config environments/grid-glb-demo/forge.yaml
```

All three clusters should show `phase=running, live`.

### 6. Verify provider gateway reachability

From the Docker host, confirm the provider gateways respond (IPs
are in `.forge/state.json` under `captures`):

```console
curl -s http://<east-ip>:8080/health
curl -s http://<west-ip>:8080/health
```

Both should return `ok` from the mock-inference health endpoint.

### 7. Start host services (blocked on images)

The two host services are defined in `forge.yaml` with `autoStart:
false` and placeholder image tags (`sha-PLACEHOLDER`). Once real images
are published, replace the tags and start the services:

```console
praxis-forge service start grid-overlay-sync-us-east --config environments/grid-glb-demo/forge.yaml
praxis-forge service start grid-edge-us-east --config environments/grid-glb-demo/forge.yaml
```

The overlay-sync service writes `grid-config.json` to
`environments/grid-glb-demo/.forge/runtime/edge-us-east/`. The edge service mounts that directory
read-only at `/etc/grid` and begins accepting requests on
`127.0.0.1:8080`.

**Remaining blockers:**

- `ghcr.io/praxis-proxy/grid-overlay-sync` image must be published
  with a ConfigMap watcher that writes `grid-config.json`
- `ghcr.io/praxis-proxy/praxis-ai` image must be published with
  file-based Grid config hot-reload support

### 8. Send a test request (blocked on step 7)

Once both services are running:

```console
curl -s http://127.0.0.1:8080/v1/chat/completions \
  -H "Content-Type: application/json" \
  -d @environments/grid-glb-demo/fixtures/requests/shared-model.json
```

### Shared Runtime Layout

Both host services share a runtime directory on the Docker host:

```
environments/grid-glb-demo/.forge/runtime/edge-us-east/
  grid-config.json    # written by overlay-sync, read by edge
  tls/                # reserved for future mTLS certificates
```

This directory is created at service start time. It is gitignored
(`.forge/` in root `.gitignore`) and must not be committed.

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
