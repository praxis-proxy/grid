# Adding an Inference Provider

Step-by-step workflow for adding a new inference provider to an existing
AGN installation. Covers in-cluster backends, existing services, and
external HTTPS APIs.

## Prerequisites

- A running AGN installation with `install.sh` (see the
  [Existing-Cluster Helm Installation](installation/existing-clusters.md))
- `kubectl`, `helm`, `jq`, `python3`, `yq` (v4.18.0+)
- Cluster access via `KUBECONFIG` and `--context`
- TLS Secrets (`consumer-tls`, `provider-tls`) already created
- The provider's credential token

## Resource Ownership

Every provider addition touches these resources. Understanding who owns
each resource prevents conflicts and simplifies troubleshooting.

| Resource | Owner | Created by |
|----------|-------|------------|
| Backend Deployment + Service | `grid-mock-providers` chart (or user) | `install.sh` or manual |
| Credential Secret | User | `kubectl create secret` |
| InferenceProvider CR | `grid-site` chart | `install.sh` |
| Overlay ConfigMap | AGN Operator (read-only) | Automatic after CR reconciliation |
| Provider Praxis config | Installer | `install.sh` `render_provider_config` |
| Consumer Praxis config | Installer | `install.sh` (direct copy) |
| Provider gateway mounts | `praxis-gateway` chart | `install.sh` |
| Consumer gateway overlay | `praxis-gateway` chart | `install.sh` |
| NetworkPolicy | `grid-mock-providers` chart (or user) | `install.sh` or manual |

## Architecture

```
            client request
                 │
                 ▼
     consumer gateway (:8080)
         │ intelligent_route
         │ reads overlay (operator-generated)
         │ selects candidate by model + score
         │
         │ mTLS
         ▼
     provider gateway (:8443)
         │ peer_identity_trust (cert_digest + org)
         │ provider_route (candidate_id → backend)
         │ credential_inject (per-route Secret)
         │
    ┌────┴────────┐
    ▼             ▼
 backend-a     backend-b
 (mock/vLLM)   (mock/vLLM)
 :8080         :8080
 cred-a        cred-b
```

The overlay assigns each InferenceProvider a deterministic `stable_id`
(FNV-1a hash). The provider gateway's `provider_route` filter uses this
`stable_id` as `candidate_id` to route requests to the correct backend.

## Workflow: Add via install.sh (Recommended)

This is the production workflow. All changes are declarative via Helm
values files and applied idempotently by `install.sh`.

### 1. Create the credential Secret

```bash
printf '%s' 'your-token-value' \
  | kubectl create secret generic my-new-credential \
      --from-file=token=/dev/stdin \
      --dry-run=client -o yaml \
  | kubectl apply --context "$CONTEXT" -n grid-system -f -
```

Use `printf '%s'` (not `echo`) to avoid trailing newlines. The
`credential_inject` filter trims whitespace, but backend pods reading
the token via environment variables see raw bytes.

### 2. Update grid-mock-providers values (in-cluster backend only)

Add the new provider to the `providers` array:

```yaml
# {site}-grid-mock-providers.yaml
providers:
  - name: existing-provider
    credentialSecret: existing-credential
    credentialKey: token
  - name: new-provider                  # creates mock-inference-new-provider
    credentialSecret: my-new-credential
    credentialKey: token
    env:
      MOCK_PROVIDER_SITE: "east1-new"
      MOCK_QUEUE_DEPTH: "0.15"
```

The chart creates a Deployment (`mock-inference-{name}`) and Service
(`mock-inference-{name}`) per provider, and one shared NetworkPolicy.

Skip this step for existing in-cluster services or external APIs.

### 3. Update grid-site values

Add the InferenceProvider to the `inferenceProviders` array:

```yaml
# {site}-grid-site.yaml
inferenceProviders:
  - name: existing-mock-provider
    gridNetworkRef: my-grid
    providerKind: self_hosted
    backendKind: local
    endpoint: "http://mock-inference-existing.grid-system.svc.cluster.local:8080"
    siteSelector:
      matchLabels:
        grid.praxis.fast/provider-site: east1
    models:
      - name: sim-model-v1
        capabilities: [text_generation]

  - name: my-new-provider                    # new
    gridNetworkRef: my-grid
    providerKind: self_hosted
    backendKind: local
    endpoint: "http://mock-inference-new-provider.grid-system.svc.cluster.local:8080"
    siteSelector:
      matchLabels:
        grid.praxis.fast/provider-site: east1
    models:
      - name: sim-model-v2
        capabilities: [text_generation]
```

Required fields: `name`, `gridNetworkRef`, `providerKind`,
`backendKind`, `endpoint`. Optional: `models`, `auth`, `healthCheck`,
`tls`, `metricsConfig`, `cost`, `accessPolicy`.

Set `spec.tls` to share the provider CA bundle and optional client certificate
across health checks, metrics scraping, and model discovery. Feature-specific
TLS settings override it when an endpoint needs different trust material or a
different client identity; see [TLS and mTLS](architecture/crds.md#tls-and-mtls).

### 4. Update provider Praxis config template

Add a route, credential injection entry, and load balancer cluster for
the new provider. Use the InferenceProvider CR name as the
`candidate_id` placeholder; `install.sh`'s `render_provider_config`
replaces it with the overlay's `stable_id` after reconciliation.

```yaml
# {site}-provider-praxis.yaml
filter_chains:
  - name: provider-inference
    filters:
      - filter: provider_route
        routes:
          - candidate_id: existing-mock-provider    # replaced by stable_id
            model: sim-model-v1
            cluster: existing-backend
            credential:
              strategy: bearer_token
              secretRef:
                name: existing-credential
                key: token
          - candidate_id: my-new-provider            # new route
            model: sim-model-v2
            cluster: new-backend
            credential:
              strategy: bearer_token
              secretRef:
                name: my-new-credential
                key: token
      - filter: credential_inject
        credentials:
          - strategy: bearer_token
            name: existing-credential
            file: /etc/praxis/credentials/existing-credential/token
          - strategy: bearer_token                   # new
            name: my-new-credential
            file: /etc/praxis/credentials/my-new-credential/token
      - filter: load_balancer
        clusters:
          - name: existing-backend
            endpoints:
              - "mock-inference-existing.grid-system.svc.cluster.local:8080"
          - name: new-backend                        # new
            endpoints:
              - "mock-inference-new-provider.grid-system.svc.cluster.local:8080"
```

### 5. Update consumer Praxis config

Add the new provider to `provider_hop_clusters` and `load_balancer.clusters`:

```yaml
# {site}-consumer-praxis.yaml
filter_chains:
  - name: main
    filters:
      - filter: intelligent_route
        provider_hop_clusters:
          - existing-mock-provider
          - my-new-provider              # new
      - filter: load_balancer
        clusters:
          - name: existing-mock-provider
            tls: { ... }
            endpoints:
              - "provider-gateway.grid-system.svc.cluster.local:8443"
          - name: my-new-provider        # new, same provider gateway endpoint
            tls:
              ca:
                ca_path: /etc/praxis/tls/ca.crt
              client_cert:
                cert_path: /etc/praxis/tls/tls.crt
                key_path: /etc/praxis/tls/tls.key
              sni: east1-provider.grid.internal
              verify: true
            endpoints:
              - "provider-gateway.grid-system.svc.cluster.local:8443"
```

Both clusters route through the same provider gateway. The
`provider_route` filter on the provider side selects the correct
backend based on the overlay candidate's `stable_id`.

### 6. Update provider gateway values

Add the new credential mount:

```yaml
# {site}-provider-gateway.yaml
credentials:
  - name: existing-credential
    mountPath: /etc/praxis/credentials/existing-credential
    optional: false
  - name: my-new-credential              # new
    mountPath: /etc/praxis/credentials/my-new-credential
    optional: false
```

Never add credential mounts to the consumer gateway.

### 7. Run install.sh

```bash
KUBECONFIG=/path/to/kubeconfig \
  bash install.sh inventory.yaml
```

`install.sh` uses `helm upgrade --install`, so it is idempotent. It:
1. Upgrades grid-operator (no-op if unchanged)
2. Upgrades grid-mock-providers (creates new backend)
3. Upgrades grid-site (creates new InferenceProvider CR)
4. Waits for overlay (operator reconciles, assigns stable_id)
5. Renders provider config (reads stable_id, replaces placeholder)
6. Creates consumer config
7. Upgrades provider gateway (picks up new credential mount)
8. Upgrades consumer gateway (no-op if overlay name unchanged)

### 8. Verify

```bash
# Check InferenceProvider status
kubectl get inferenceproviders -n grid-system

# Verify overlay has the new candidate
kubectl get configmap -l grid.praxis.fast/network \
  -n grid-system -o jsonpath='{.items[0].data.routing-config\.json}' \
  | jq -r '.candidates[] | .name + " stable_id=" + .stable_id'

# Test routing
curl -s -H "Content-Type: application/json" \
  -d '{"model":"sim-model-v2","messages":[{"role":"user","content":"test"}]}' \
  http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions

# Run verify.sh
bash verify.sh inventory.yaml
```

### Service Naming

Use `fullnameOverride` in gateway values for stable, predictable
Service names:

```yaml
# provider-gateway values
fullnameOverride: provider-gateway

# consumer-gateway values
fullnameOverride: consumer-gateway
```

This produces Service names `provider-gateway` and `consumer-gateway`.
Without `fullnameOverride`, Helm generates names like
`{release-name}-praxis-gateway` (e.g. `provider-gateway-praxis-gateway`),
which couples consumer and provider configs to Helm release names.

Existing installations that omit `fullnameOverride` work correctly but
use release-generated names. Adding `fullnameOverride` to an existing
installation changes the Service name and requires updating all
references (operator `gateway.serviceName`, consumer `load_balancer`
endpoints, provider TLS SANs).

**Recommendation**: Use `fullnameOverride` for new installations.
Preserve release-generated names in existing installations unless you
are prepared for the service name migration.

## Provider Type: Existing In-Cluster Service

When the backend already runs as a Kubernetes Service (vLLM, TGI,
Ollama), skip the `grid-mock-providers` chart:

1. Create the credential Secret
2. Add an InferenceProvider CR pointing to the existing Service endpoint:
   ```yaml
   endpoint: "http://my-vllm.inference.svc.cluster.local:8000"
   ```
3. Add the route, credential, and cluster to provider Praxis config
4. Add the hop cluster to consumer Praxis config
5. Add the credential mount to provider gateway values
6. Ensure a NetworkPolicy permits ingress from `provider-gateway` and
   `grid-operator` pods to the backend Service port

The operator probes `spec.endpoint` for health checks. If a
NetworkPolicy blocks the operator, the provider stays `Unavailable`
and the overlay excludes it.

## Provider Type: External HTTPS API

External APIs (OpenAI, Anthropic, Bedrock) have no backend Deployment,
Service, or NetworkPolicy in the cluster. The provider gateway connects
directly to the external endpoint over HTTPS.

1. Create the credential Secret (API key)
2. Add an InferenceProvider CR:
   ```yaml
   - name: openai-east1-provider
     gridNetworkRef: my-grid
     providerKind: External
     backendKind: OpenAI
     endpoint: "https://api.openai.com"
     models:
       - name: gpt-4o-mini
         capabilities: [text_generation]
   ```
3. Update provider Praxis config with external-specific fields:
   ```yaml
   routes:
     - candidate_id: openai-east1-provider
       model: gpt-4o-mini
       cluster: openai-backend
       credential:
         strategy: bearer_token
         secretRef:
           name: openai-credential
           key: token

   load_balancer:
     clusters:
       - name: openai-backend
         authority: api.openai.com        # HTTP Host header
         tls:
           sni: api.openai.com            # independent of internal provider SNI
           verify: true                    # uses system CA trust store
         endpoints:
           - "api.openai.com:443"
   ```
4. Add the hop cluster to consumer Praxis config
5. Add the credential mount to provider gateway values
6. Run `install.sh`

Key differences from in-cluster providers:

| Field | In-Cluster | External HTTPS |
|-------|-----------|----------------|
| `authority` | Not needed (uses endpoint hostname) | Required (external hostname) |
| `tls.sni` | Internal SNI (e.g. `east1-provider.grid.internal`) | External hostname (e.g. `api.openai.com`) |
| `tls.verify` | `true` with `ca_path` to private CA | `true` without `ca:` block (system CA) |
| `tls.ca` | `ca_path: /etc/praxis/tls/ca.crt` | Omit (system trust store) |
| Backend Deployment | Chart-managed or user-managed | None |
| NetworkPolicy | Required (provider-gateway + operator ingress) | None |
| Endpoint | `http://service.namespace.svc:port` | `https://api.example.com` |

`ca_system` is not a valid Praxis configuration field. Omit the `ca:`
block entirely to use the system trust store for public APIs.

## TLS Secret Key Contract

The `praxis-gateway` chart mounts the entire TLS Secret at
`/etc/praxis/tls/` without specifying individual items. Praxis
configuration references three files:

| Key | Used by | Purpose |
|-----|---------|---------|
| `tls.crt` | Both gateways | Server certificate (provider) / client certificate (consumer) |
| `tls.key` | Both gateways | Private key |
| `ca.crt` | Both gateways | CA certificate for peer verification |

A standard `kubernetes.io/tls` Secret provides only `tls.crt` and
`tls.key`. The `ca.crt` key must be added explicitly:

```bash
kubectl create secret generic provider-tls \
  --from-file=tls.crt=provider.crt \
  --from-file=tls.key=provider.key \
  --from-file=ca.crt=ca.crt \
  -n grid-system --context "$CONTEXT"
```

## Credential Isolation

Each provider uses a distinct credential Secret and mount path.
Credentials are mounted only on the provider gateway, never on the
consumer gateway or operator.

```
provider-gateway pod:
  /etc/praxis/credentials/credential-a/token  ← Secret credential-a
  /etc/praxis/credentials/credential-b/token  ← Secret credential-b

consumer-gateway pod:
  (no credential mounts)

grid-operator pod:
  (no credential mounts)
```

The `provider_route` filter maps each route to its credential via
`secretRef.name`. The `credential_inject` filter reads the
corresponding file from the mount path.

## Stable ID Discovery

The overlay assigns each InferenceProvider a deterministic `stable_id`
(FNV-1a hash of `{kind}/{name}/{site}/{cluster}`). This value is
NOT the CR's `.metadata.name`.

After running `install.sh` (which waits for the overlay), inspect
the assigned stable IDs:

```bash
kubectl get configmap -l grid.praxis.fast/network \
  -n grid-system -o jsonpath='{.items[0].data.routing-config\.json}' \
  | jq -r '.candidates[] | .name + " stable_id=" + .stable_id'
```

`install.sh`'s `render_provider_config` automates this: it reads the
overlay, extracts stable IDs, and replaces `candidate_id` placeholders
in the provider Praxis config template. When using `install.sh`, you
write CR names as `candidate_id` values in the template and the script
handles the translation.

When adding providers manually (without `install.sh`), you must read
the stable IDs from the overlay and set them as `candidate_id` in the
provider config yourself.

## Removing a Provider

To remove a provider, delete its entries from all values files and
re-run `install.sh`:

1. Remove from `inferenceProviders` array in grid-site values
2. Remove from `providers` array in grid-mock-providers values (if chart-managed)
3. Remove the route, credential injection, and load balancer cluster from provider Praxis config
4. Remove from `provider_hop_clusters` and `load_balancer.clusters` in consumer Praxis config
5. Remove the credential mount from provider gateway values
6. Run `install.sh`

Helm removes the InferenceProvider CR, backend Deployment and Service
(if chart-managed), and credential volume mount. The operator updates
the overlay to exclude the removed candidate. Remaining providers and
routes are unaffected.

User-managed Secrets (credentials, TLS) are not deleted by Helm. Delete
them explicitly if no longer needed:

```bash
kubectl delete secret my-removed-credential -n grid-system --context "$CONTEXT"
```

## NetworkPolicy

The `grid-mock-providers` chart creates a NetworkPolicy that permits
ingress from two sources:

1. **Provider gateway** pods (`app.kubernetes.io/instance: provider-gateway`)
   for request forwarding
2. **AGN Operator** pods (`app.kubernetes.io/name: grid-operator`)
   for health check probes

If the operator cannot reach a backend's endpoint, the InferenceProvider
stays `Unavailable` and the overlay excludes it.

For backends not managed by the `grid-mock-providers` chart, create a
NetworkPolicy that permits these two sources. See
[NetworkPolicy for Multiple Backends](installation/existing-clusters.md#networkpolicy-for-multiple-backends).

## Troubleshooting

### InferenceProvider stays Unavailable after adding

**Cause**: Backend not reachable (not running, wrong endpoint, blocked
by NetworkPolicy).

**Check**:
```bash
kubectl get inferenceproviders -n grid-system
kubectl logs -l app.kubernetes.io/name=grid-operator -n grid-system | grep -i "unavailable\|error\|health"
```

**Fix**: Verify the backend pod is running and the endpoint in the
InferenceProvider CR resolves to a healthy service. Ensure
NetworkPolicy allows grid-operator ingress.

### Overlay missing new candidate

**Cause**: InferenceProvider CR not reconciled, or `siteSelector` does
not match any GridSite.

**Check**:
```bash
kubectl get configmap -l grid.praxis.fast/network -n grid-system \
  -o jsonpath='{.items[0].data.routing-config\.json}' | jq '.candidates[].name'
```

**Fix**: Verify the GridSite has the
`grid.praxis.fast/provider-site` label matching the
InferenceProvider's `siteSelector.matchLabels`.

### Consumer returns 500 for new model

**Cause**: Missing `provider_hop_clusters` or `load_balancer.clusters`
entry in consumer Praxis config.

**Fix**: Add the InferenceProvider CR name to both
`provider_hop_clusters` and `load_balancer.clusters` in the consumer
Praxis config.

### Provider gateway rejects requests with 403

**Cause**: `candidate_id` in provider config does not match the
overlay's `stable_id`. This happens when using CR names instead of
stable IDs without `render_provider_config`.

**Fix**: Re-run `install.sh` to trigger `render_provider_config`, or
manually read the stable ID from the overlay and update `candidate_id`.

### Credential mismatch (401 from backend)

**Cause**: Provider gateway mounts the wrong Secret, or the Secret
contains a trailing newline.

**Fix**: Verify the `secretRef.name` in the provider route matches the
credential mount name in provider gateway values. Recreate the Secret
with `printf '%s'` to strip trailing newlines.
