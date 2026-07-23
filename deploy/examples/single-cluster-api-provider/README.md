# Single Cluster API Provider Example

This example demonstrates a minimal Grid deployment with:
- One `GridNetwork` and `GridSite` 
- One `InferenceProvider` pointing to an external API
- Consumer gateway configuration (Praxis AI gateway deployment separate)

This example is not runnable end-to-end by itself. It requires a valid external
API credential and a separately deployed Praxis AI gateway that consumes the
Grid-generated ConfigMap.

## Prerequisites

1. Grid operator installed (see `../../README.md`)
2. Praxis AI gateway deployment with:
   - `grid_route` filter
   - `grid_credential_inject` filter  
   - Consumer configuration referencing generated ConfigMaps

## Installation

```bash
# 1. Apply Grid resources
kubectl apply -f gridnetwork.yaml
kubectl apply -f gridsite.yaml
kubectl apply -f inference-provider.yaml

# 2. Create credential Secret (replace with real token)
kubectl create secret generic anthropic-api-key \
  --from-literal=api-key="sk-ant-api03-..." \
  --namespace=default

# 3. Verify operator generates overlay ConfigMap
kubectl get configmap grid-overlay-example-consumer-gateway -o yaml

# 4. Deploy Praxis AI gateway (separate - not included here)
#    Must reference the generated ConfigMap above
```

## Generated Resources

The Grid operator will create:
- `ConfigMap/grid-overlay-example-consumer-gateway` - routing overlay for Praxis AI
- `Secret/grid-ca-cert` - Grid CA certificate (auto-generated)  
- `Secret/grid-site-cert` - site certificate for this cluster (auto-generated)

## What This Proves

✅ Grid CRDs can be applied  
✅ Operator processes resources without errors  
✅ RBAC allows Secret/ConfigMap operations  
✅ Routing overlay generation works  
✅ Credential reference validation works  

❌ This does NOT test end-to-end routing (requires Praxis AI gateway deployment)

## Notes

- **Credential**: Replace the example token with a real Anthropic API key
- **Endpoint**: Uses Anthropic's production API (api.anthropic.com)
- **Praxis Gateway**: Must be deployed separately with Grid-compatible configuration
- **No mTLS**: This example uses simple API key auth, no inter-site mTLS
