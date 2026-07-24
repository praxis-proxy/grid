# Grid GLB Demo Environment

Multi-cluster Grid environment skeleton demonstrating external ingress
with GLB-style failover. Three Kind clusters simulate a realistic
topology: one edge-control plane running Praxis AI as the external entry
point, and two provider clusters hosting simulated inference backends.

## Architecture

```
                   ┌──────────────────┐
                   │   Client (curl)  │
                   └────────┬─────────┘
                            │ :8080
               ┌────────────▼────────────┐
               │   grid-edge-us-east     │
               │   (Praxis AI gateway)   │
               └────────────┬────────────┘
                            │ overlay routing
            ┌───────────────┼───────────────┐
            ▼               ▼               ▼
   ┌─────────────┐  ┌──────────────┐  ┌──────────────┐
   │ edge-control │  │provider-east │  │provider-west │
   │  (Kind)      │  │  (Kind)      │  │  (Kind)      │
   │  GridNetwork │  │  GridSite    │  │  GridSite    │
   │  Operator    │  │  Operator    │  │  Operator    │
   │              │  │  mock-provs  │  │  mock-provs  │
   └─────────────┘  └──────────────┘  └──────────────┘
```

The `grid-overlay-sync-us-east` host service watches the edge-control
cluster for the operator-generated overlay ConfigMap and writes it to a
shared directory. The `grid-edge-us-east` service reads that config and
routes inference requests to provider clusters via the Grid overlay.

## Current Status

This skeleton validates and defines the complete cluster, stack, and
service topology. The following items are **not yet runnable** and are
blocked on upstream packaging:

- **grid-overlay-sync image** does not yet exist (`sha-PLACEHOLDER`).
  The overlay-sync service requires a packaged watcher that reads the
  operator-generated ConfigMap and writes grid-config.json.
- **Praxis AI hot-reload image** does not yet exist (`sha-PLACEHOLDER`).
  The edge service requires a Praxis AI build with file-based Grid
  config hot-reload support.
- **Provider gateway endpoints** (`PROVIDER_EAST_LB:443`,
  `PROVIDER_WEST_LB:443` in gridnetwork.yaml) are placeholders. Each
  provider cluster needs a Praxis gateway Deployment + LoadBalancer
  Service exposed via MetalLB before the edge can route to them.
- **Transport** between edge and providers is set to `plaintext` for
  initial development. Production requires `mutual_tls` with proper
  SNI and cert references.

## Prerequisites

- Docker (required for cross-cluster networking; Podman is not supported)
- [kind](https://kind.sigs.k8s.io/) v0.20+
- `praxis-forge` binary (built from this repo: `cargo build -p forge`)
- `kubectl`
- `helm` (not currently used but reserved for future chart steps)

## What Works Today

### 1. Validate the environment config

```console
praxis-forge config validate --config environments/grid-glb-demo/forge.yaml
```

### 2. Bring up clusters and infrastructure stacks

```console
praxis-forge up --config environments/grid-glb-demo/forge.yaml
```

This creates three Kind clusters with cross-cluster networking, installs
Gateway API CRDs, MetalLB with auto-configured address pools, the Grid
operator (CRDs + deployment), and applies per-cluster Grid CRD resources
(GridNetwork on edge-control, GridSites and InferenceProviders on
provider clusters).

The host services (`grid-overlay-sync-us-east`, `grid-edge-us-east`)
will fail to start until the placeholder images are replaced with real
builds.

### 3. Verify cluster status

```console
praxis-forge status --config environments/grid-glb-demo/forge.yaml
```

All three clusters should show `phase=running, live`. Services will
show as failed until the images exist.

### 4. Inspect service identity (once services are running)

```console
praxis-forge status --json --config environments/grid-glb-demo/forge.yaml
```

Service entries include `containerId`, `startedAt`, and `restartCount`
from live container inspect.

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
