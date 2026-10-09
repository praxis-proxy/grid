# Existing-Cluster Helm Installation

This Helm-based installation workflow installs the AGN Operator and Praxis
gateways on existing Kubernetes clusters. Two topology layouts use the
same charts and installer scripts. The workflow has been validated through
chart rendering, disposable local clusters, and existing single-node
Kubernetes clusters.

Review the [technical preview scope and limitations](../technical-preview-scope.md)
before planning cluster connectivity, provider deployment, and gateway roles.

## Guides

- **[Adding an Inference Provider](../adding-provider.md)** —
  step-by-step workflow for adding in-cluster, existing-service, or
  external HTTPS providers to a running AGN installation.

## Topologies

### Dedicated Logical Edge Gateways

Consumer and provider responsibilities on separate clusters. Consumer
clusters run only Praxis consumer gateways; provider clusters run
Praxis provider gateways alongside inference backends.

See `examples/helm/existing-clusters/dedicated-edge/` for site-specific Helm values.

### Combined Consumer and Provider Sites

Every cluster runs both consumer and provider gateway roles. Separate Praxis
Deployments, Services, configuration, and credentials preserve the role
separation. The complete provider boundary also depends on the configured
mTLS trust, RBAC, and NetworkPolicy when both roles share a cluster.

See `examples/helm/existing-clusters/combined-site/` for site-specific Helm values.

## Inventory

Copy `examples/helm/existing-clusters/inventory.example.yaml` to a local, gitignored `inventory.yaml`
and fill in your cluster contexts, reachable gateway addresses, and
SWIM service addresses:

```bash
cp examples/helm/existing-clusters/inventory.example.yaml inventory.yaml
# Edit inventory.yaml with your values
```

The inventory is never committed. All scripts read it at runtime.

## Prerequisites

The scripts require `kubectl`, Helm, `jq`, `python3`, and
[Mike Farah `yq`](https://github.com/mikefarah/yq#install) version 4.18.0 or
newer. The preflight rejects other `yq` implementations and older versions
before parsing the inventory. Install guidance:

- `yq`: https://github.com/mikefarah/yq#install
- `jq`: https://jqlang.github.io/jq/download/
- `python3`: https://www.python.org/downloads/ or your OS package manager

Before running the installer, prepare the following in each cluster's
`grid-system` namespace.

| Resource | Ownership | Purpose |
|----------|-----------|---------|
| `consumer-praxis-config` ConfigMap | installer-managed | Created by `install.sh` from config template |
| `provider-praxis-config` ConfigMap | installer-managed | Created by `install.sh` from config template with overlay stable IDs |
| `consumer-tls` Secret | user-managed | TLS certificate and key for consumer identity |
| `provider-tls` Secret | user-managed | TLS certificate and key for provider identity |
| Per-provider credential Secrets | user-managed | One Secret per inference provider backend |

AGN topology CRs (`GridNetwork`, `GridSite`, `InferenceProvider`) and mock
inference backends are now managed by the `grid-site` and
`grid-mock-providers` Helm charts respectively.

The routing overlay ConfigMap is created automatically by the AGN
Operator once SWIM membership converges. Its name follows the pattern
`grid-overlay-{network}-{gateway}`, where `{network}` is the
GridNetwork CR name and `{gateway}` is the consumer gateway Service
name (set by `fullnameOverride` in the gateway values or derived from
the Helm release name).

## Usage

```bash
# Verify prerequisites
examples/helm/existing-clusters/scripts/preflight.sh inventory.yaml

# Install AGN + Praxis on all clusters
examples/helm/existing-clusters/scripts/install.sh inventory.yaml

# Install with per-site overrides
examples/helm/existing-clusters/scripts/install.sh inventory.yaml \
  --site-values east2:operator:/path/to/operator-overrides.yaml \
  --site-values east2:consumer:/path/to/consumer-overrides.yaml

# Run verification
examples/helm/existing-clusters/scripts/verify.sh inventory.yaml

# Clean up (does not delete namespace or CRDs)
examples/helm/existing-clusters/scripts/uninstall.sh inventory.yaml
```

Every script requires the inventory file as the first argument. All
commands use explicit `--kube-context` selection and never modify the
user's current context.

## Configuration Layers

Values are merged in Helm's last-wins order:

1. **Chart defaults** — `charts/*/values.yaml`
2. **Topology example values** — `examples/helm/existing-clusters/{topology}/values/{site}-{role}.yaml`
3. **User override files** — via `--site-values` (see below)
4. **Inventory image overrides** — `.images.operator` / `.images.gateway`
   in the inventory, applied as `--set`
5. **Explicit `--set`** — reserved for SWIM seeds (auto-computed)

Later sources override earlier ones. Use override files for structured
configuration and reserve `--set` for small scalar changes.

The final Helm invocation for a combined-site consumer gateway is:

```bash
helm upgrade --install consumer-gateway charts/praxis-gateway \
  --namespace grid-system \
  --values examples/helm/existing-clusters/combined-site/values/east-a-consumer-gateway.yaml \
  --values /path/to/my-east-a-consumer-overrides.yaml \
  --set image.repository=... \
  --set image.tag=...
```

Do not edit tracked example values files. Use override files instead.

## User Overrides

The installer accepts repeatable `--site-values` arguments to overlay
user-provided values on top of repository defaults:

```bash
examples/helm/existing-clusters/scripts/install.sh inventory.yaml \
  --site-values east2:operator:/path/to/operator.yaml \
  --site-values east2:consumer:/path/to/consumer.yaml \
  --site-values east2:provider:/path/to/provider.yaml
```

Format: `SITE:ROLE:PATH` where:

- **SITE** matches a key in `inventory.yaml` `.sites`
- **ROLE** is `operator`, `site`, `mock`, `consumer`, or `provider`
- **PATH** is a readable YAML file

The installer validates all overrides before modifying any cluster:

- Unknown sites or roles are rejected
- Missing or unreadable files are rejected
- Duplicate `SITE:ROLE` pairs are rejected
- For dedicated-edge topology, the role must match the site's declared role

Override file paths and SHA-256 digests (never contents) are logged
for traceability.

## Install Ordering

For combined-site topology, the installer deploys components in this
order per site:

1. `grid-operator` — registers SWIM identity, installs CRDs, reconciles CRs
2. `grid-mock-providers` — deploys mock inference backends (optional, skipped if no values file)
3. `grid-site` — creates GridNetwork, GridSite, and InferenceProvider CRs
4. **Overlay wait** — polls until the operator creates the overlay
   ConfigMap (up to 120 seconds)
5. **Provider config** — reads overlay stable IDs and renders provider
   Praxis config from template
6. `provider-gateway` — serves inference routes over mTLS
7. `consumer-gateway` — mounts the overlay for intelligent routing

The operator needs CRDs before CRs can be applied (step 3 depends on
step 1). Mock backends must be healthy when InferenceProvider CRs
trigger the operator's health checks (step 2 before step 3).
The provider config needs stable IDs from the overlay (step 5 depends
on step 4). The consumer gateway volume-mounts the overlay ConfigMap
(step 7 depends on step 4).

For dedicated-edge topology, consumer and provider are on separate
clusters, but the grid-site and grid-mock-providers releases still
install between the operator and gateways.

## Charts

Both topologies use the same charts:

- `charts/grid-operator` -- AGN Operator with SWIM, CRD management
- `charts/grid-site` -- AGN topology CRs (`GridNetwork`, `GridSite`, `InferenceProvider`)
- `charts/grid-mock-providers` -- Mock inference backends, Services, NetworkPolicy
- `charts/praxis-gateway` -- Praxis AI Gateway (consumer or provider role)

## Security Boundary

Regardless of topology, the consumer and provider gateway roles maintain
separate trust boundaries:

- Consumer gateways receive routing overlays and client requests
- Provider gateways authenticate consumer peers via mTLS
- Provider credentials are mounted only in provider gateway Deployments
- Provider credentials must never be mounted into consumer gateways

## Candidate Identity

The provider gateway's `provider_route` filter requires a `candidate_id` for
each route. This value must match the `stable_id` in the routing overlay — it
is **not** the InferenceProvider CR `.metadata.name`.

The operator computes `stable_id` as a deterministic FNV-1a hash of
`{kind}/{name}/{site}/{cluster}`. After the overlay ConfigMap converges, read
the stable IDs from the overlay data:

```bash
kubectl get configmap -l grid.praxis.fast/network \
  -n grid-system -o jsonpath='{.items[0].data.routing-config\.json}' \
  | jq -r '.candidates[] | .name + " stable_id=" + .stable_id'
```

Use the printed `stable_id` values as `candidate_id` in the provider Praxis
configuration.

## GridSite Labels

Each GridSite must carry the `grid.praxis.fast/provider-site` label
matching the value used in InferenceProvider `siteSelector.matchLabels`.
Without this label, the operator finds no matching sites for the
InferenceProvider, produces zero overlay candidates, and never creates
the overlay ConfigMap.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridSite
metadata:
  name: east2
  labels:
    grid.praxis.fast/provider-site: east2
spec:
  gridNetworkRef: my-grid
  region: us-east-2
  zone: us-east-2a
```

The preflight script checks for this label and fails if no GridSite in
the namespace has it.

## TLS Certificate Requirements

Each site needs TLS certificates with site-specific Subject Alternative
Names (SANs) that match the `sni` field in the consumer Praxis
configuration.

For a site named `east2` with the consumer config
`sni: east2-provider.grid.internal`:

- **provider-tls** must include SAN `DNS:east2-provider.grid.internal`
- **consumer-tls** must include SAN `DNS:east2-consumer.grid.internal`
- Both certs must be signed by the same CA used in `client_ca.ca_path`

Reusing another site's certificates causes TLS handshake failures:

```
invalid peer certificate: certificate not valid for name
"east2-provider.grid.internal"; certificate is only valid for
DnsName("east1-provider.grid.internal")
```

The preflight script inspects TLS Secret SANs and prints them for
manual verification. Generate per-site certificates, naming the site in both the
organization and the common name, since a receiving gateway authorizes on the
organization:

```bash
openssl ecparam -genkey -name prime256v1 -noout -out provider.key
openssl req -new -key provider.key \
  -subj "/O=east2-provider/CN=east2-provider" \
  -addext "subjectAltName=DNS:provider-gateway.grid-system.svc.cluster.local,DNS:east2-provider.grid.internal" \
  -addext "keyUsage=digitalSignature,keyEncipherment" \
  -addext "extendedKeyUsage=clientAuth,serverAuth" \
  -out provider.csr
openssl x509 -req -in provider.csr \
  -CA ca.crt -CAkey ca.key -CAcreateserial \
  -days 365 -copy_extensions copyall \
  -out provider.crt
```

## Consumer Cluster Coverage

The consumer gateway's `provider_hop_clusters` and `load_balancer.clusters`
must include an entry for every provider cluster that the overlay might
route requests to. Each InferenceProvider CR creates an overlay candidate
with a `cluster` field set to the CR name.

If a request is routed to an overlay candidate whose cluster is not
configured in the consumer's load_balancer, the consumer returns
HTTP 500:

```
load_balancer filter: unknown cluster 'openai-east2-provider'
```

When adding a new InferenceProvider (such as an external OpenAI
provider), add a corresponding cluster entry to the consumer Praxis
config:

```yaml
# In consumer-praxis.yaml
filter_chains:
  - name: main
    filters:
      - filter: intelligent_route
        provider_hop_clusters:
          - mock-east2-provider
          - openai-east2-provider      # must match InferenceProvider name
      - filter: load_balancer
        clusters:
          - name: mock-east2-provider
            # ... existing mock config
          - name: openai-east2-provider
            tls:
              ca:
                ca_path: /etc/praxis/tls/ca.crt
              client_cert:
                cert_path: /etc/praxis/tls/tls.crt
                key_path: /etc/praxis/tls/tls.key
              sni: east2-provider.grid.internal
              verify: true
            endpoints:
              - "provider-gateway.grid-system.svc.cluster.local:8443"
```

The verify script cross-checks overlay candidate clusters against the
consumer config and warns about missing entries.

## Credential Preparation

Credential Secrets must not contain trailing newlines. The `credential_inject`
filter trims whitespace before injection, but other consumers of the Secret
data (such as mock-inference pods reading token environment variables) see the
raw bytes.

Strip trailing newlines when creating credential Secrets:

```bash
printf '%s' "$(cat /path/to/token)" \
  | kubectl create secret generic my-credential \
      --from-file=token=/dev/stdin \
      --dry-run=client -o yaml \
  | kubectl apply --context "$CONTEXT" -n grid-system -f -
```

## External Provider Configuration

When routing to external APIs (such as OpenAI), the provider gateway's
`load_balancer` cluster configuration requires:

- **`authority`**: Sets the HTTP Host header for upstream connections. Must
  match the external hostname (e.g., `authority: api.openai.com`). Without
  this field, the gateway sends the internal service name as the Host header,
  which external CDNs reject.

- **`tls.sni`**: Must match the `authority` hostname.

- **`tls.verify: true`** without a `ca:` block: Uses the system default trust
  store, which includes public CA roots. This is the correct configuration for
  public API endpoints. Use `ca_path` only when connecting to services with
  private or custom certificate authorities.

`ca_system` is not a valid Praxis configuration field.

## Mock Inference Backend (Optional)

The `grid-mock-providers` chart is a **validation tool**, not a
production requirement. It deploys simulated inference backends so you
can verify the full routing pipeline — mTLS, credential injection,
overlay convergence, NetworkPolicy — without real model infrastructure.

**When to use mock providers:**
- First-time installation validation
- CI/CD pipeline verification
- Development and testing environments

**Production alternatives — replace mock providers with:**
- **In-cluster inference services** such as vLLM, TGI, or Ollama
  running as Deployments with matching InferenceProvider CRs
- **Externally managed HTTPS inference APIs** such as OpenAI,
  Anthropic, or Bedrock — configure the provider gateway's
  `load_balancer` clusters with the external endpoint, `authority`,
  and `tls.sni` fields (see [External Provider Configuration](#external-provider-configuration))

To skip mock providers, omit the `{site}-grid-mock-providers.yaml`
values file from your `valuesDir`. The installer only deploys
`grid-mock-providers` when a values file is present.

The `grid-mock-providers` image requires startup arguments:

```yaml
containers:
  - name: mock-inference
    image: ghcr.io/praxis-proxy/grid-mock-providers:v0.2.0
    args: ["--provider", "openai", "--port", "8080"]
    env:
      - name: MOCK_EXPECTED_BEARER_TOKEN
        valueFrom:
          secretKeyRef:
            name: mock-inference-credential
            key: token
```

Without `--provider` and `--port`, the container exits immediately with
a missing-argument error.

## Multiple Providers in One Site

A single site can host multiple independent inference providers behind
one provider gateway. Each provider gets its own Deployment, Service,
InferenceProvider CR, credential Secret, and stable ID.

```text
                 consumer gateway (:8080)
                         │
                   ┌─────┴─────┐
                   │  overlay   │  2 candidates: a82ea491, a52e9fd8
                   └─────┬─────┘
                         │ mTLS
                 provider gateway (:8443)
                    ┌────┴────┐
                    │         │
           provider_route     provider_route
           candidate a82ea491 candidate a52e9fd8
                    │         │
            ┌───────┴──┐  ┌──┴───────┐
            │ mock-a   │  │ mock-b   │
            │ :8080    │  │ :8080    │
            │ cred-a   │  │ cred-b   │
            └──────────┘  └──────────┘
```

### InferenceProvider CRs

Create one InferenceProvider per backend. Both can serve the same model
— the overlay lists both as candidates and the consumer gateway selects
between them:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: InferenceProvider
metadata:
  name: mock-west1-a         # becomes overlay candidate cluster name
spec:
  gridNetworkRef: my-grid
  providerKind: self_hosted
  backendKind: local
  endpoint: "http://mock-inference-a.grid-system.svc.cluster.local:8080"
  siteSelector:
    matchLabels:
      grid.praxis.fast/provider-site: west1
  models:
    - name: sim-model-v1
      capabilities: [text_generation]
---
apiVersion: grid.praxis.fast/v1alpha1
kind: InferenceProvider
metadata:
  name: mock-west1-b
spec:
  gridNetworkRef: my-grid
  providerKind: self_hosted
  backendKind: local
  endpoint: "http://mock-inference-b.grid-system.svc.cluster.local:8080"
  siteSelector:
    matchLabels:
      grid.praxis.fast/provider-site: west1
  models:
    - name: sim-model-v1
      capabilities: [text_generation]
```

### Per-Provider Credentials

Each provider uses a separate credential Secret and a separate mount
path. In the provider gateway Helm values:

```yaml
credentials:
  - name: mock-credential-a
    mountPath: /etc/praxis/credentials/mock-credential-a
    optional: false
  - name: mock-credential-b
    mountPath: /etc/praxis/credentials/mock-credential-b
    optional: false
```

The `provider_route` filter maps each candidate to its credential:

```yaml
routes:
  - candidate_id: a82ea491      # stable_id from overlay
    model: sim-model-v1
    cluster: mock-backend-a
    credential:
      strategy: bearer_token
      secretRef:
        name: mock-credential-a
        key: token
  - candidate_id: a52e9fd8
    model: sim-model-v1
    cluster: mock-backend-b
    credential:
      strategy: bearer_token
      secretRef:
        name: mock-credential-b
        key: token
```

### Service Naming

Each mock backend needs its own Service so the provider gateway's
`load_balancer` clusters and the operator's health checks can reach
them independently:

| Backend | Deployment | Service | Port |
|---------|------------|---------|------|
| A | `mock-inference-a` | `mock-inference-a` | 8080 |
| B | `mock-inference-b` | `mock-inference-b` | 8080 |

The chart sets `app.kubernetes.io/name: grid-mock-providers` and
`app.kubernetes.io/component: mock-inference` on both Deployments for
shared NetworkPolicy selection, and `app.kubernetes.io/instance: a` /
`b` (the provider `name`) for per-backend selectors.

### Consumer Hop Clusters

The consumer gateway must list every provider's overlay candidate
cluster in `provider_hop_clusters`:

```yaml
- filter: intelligent_route
  provider_hop_clusters:
    - mock-west1-a     # matches InferenceProvider name / overlay candidate
    - mock-west1-b
```

Each cluster also needs a `load_balancer` entry. Both clusters route
through the same provider gateway endpoint — the `provider_route`
filter on the provider side selects the correct backend based on
candidate ID:

```yaml
- filter: load_balancer
  clusters:
    - name: mock-west1-a
      tls: { ... }     # same mTLS config for both
      endpoints:
        - "provider-gateway.grid-system.svc.cluster.local:8443"
    - name: mock-west1-b
      tls: { ... }
      endpoints:
        - "provider-gateway.grid-system.svc.cluster.local:8443"
```

### NetworkPolicy for Multiple Backends

The NetworkPolicy must allow ingress from **both** the provider gateway
and the AGN Operator. The operator probes each InferenceProvider's
`spec.endpoint` for health checks — if blocked, the provider stays
`Unavailable` and the overlay has no candidates:

```yaml
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: mock-inference-provider-only
  namespace: grid-system
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: grid-mock-providers
      app.kubernetes.io/component: mock-inference
  policyTypes:
    - Ingress
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/instance: provider-gateway
        - podSelector:
            matchLabels:
              app.kubernetes.io/name: grid-operator
      ports:
        - protocol: TCP
          port: 8080
```

### Stable ID Discovery

After the overlay converges, read both candidates' stable IDs:

```bash
kubectl get configmap -l grid.praxis.fast/network \
  -n grid-system -o jsonpath='{.items[0].data.routing-config\.json}' \
  | jq -r '.candidates[] | .name + " stable_id=" + .stable_id'
```

Example output:

```
mock-west1-a stable_id=a82ea491
mock-west1-b stable_id=a52e9fd8
```

Use these `stable_id` values as `candidate_id` in the provider
gateway's `provider_route` configuration.

## Single-Cluster Trial

To validate configuration before deploying a full multi-site topology,
install one combined site first:

1. Create an inventory with a single site
2. Run preflight and install
3. Verify mock inference through the consumer gateway

This proves chart rendering, Praxis configuration, TLS, credential
isolation, and overlay convergence on one cluster. It does **not**
prove distributed SWIM membership, remote fallback, cross-site
overlay propagation, or multi-site convergence.

## Expansion

To add sites after a single-cluster trial:

1. Add new sites to the inventory with their contexts and addresses
2. Set SWIM seeds to include the existing site's SWIM address
3. Create prerequisite resources (namespace, TLS, configs, CRs) on
   the new clusters
4. Run `install.sh` — it uses `helm upgrade --install`, which is
   idempotent on the existing site and installs on new sites
5. Run `verify.sh` to confirm overlay convergence across all sites

Do not recreate the original site. The existing operator joins the
new peers via SWIM discovery.

## Common Customizations

### Images

Replace images in an override file:

```yaml
# operator-overrides.yaml
image:
  repository: my-registry.example.com/grid-operator
  tag: v0.2.0
  pullPolicy: Always
```

Use `image.digest` for immutable deployments:

```yaml
image:
  repository: ghcr.io/praxis-proxy/grid-operator
  digest: "sha256:f09712fb99d54357e0e73be5973bd865764ee9c846e076cf994c215bf4410bf4"
```

When `digest` is set, `tag` is ignored.

### Image Pull Secrets

```yaml
imagePullSecrets:
  - name: my-registry-credentials
```

### Service Names

Set `fullnameOverride` to control the provider gateway Service name. The AGN
Operator's `gateway.serviceName` must match that provider Service:

```yaml
# provider-gateway-overrides.yaml
fullnameOverride: provider-gateway

# operator-overrides.yaml
gateway:
  serviceName: provider-gateway
  port: "8443"
```

### Service Type and Ports

```yaml
service:
  type: LoadBalancer    # automatic provider-gateway discovery needs this
  port: 8443
  annotations:
    service.beta.kubernetes.io/aws-load-balancer-type: nlb
```

With a `ClusterIP` or `NodePort` provider Service, set the operator's
`gateway.address` to a reachable `host:port` instead; the operator cannot
derive a cross-site address from those Service types.

### SWIM Addresses

```yaml
swim:
  advertiseAddress: "swim.east2.example.com:7946"
  seeds: "swim.east1.example.com:7946,swim.west1.example.com:7946"
  service:
    enabled: true
    type: LoadBalancer
```

### Resources

```yaml
resources:
  requests:
    cpu: 100m
    memory: 64Mi
  limits:
    cpu: 500m
    memory: 256Mi
```

### Scheduling

```yaml
nodeSelector:
  kubernetes.io/arch: amd64

tolerations:
  - key: "dedicated"
    operator: "Equal"
    value: "inference"
    effect: "NoSchedule"

affinity:
  podAntiAffinity:
    preferredDuringSchedulingIgnoredDuringExecution:
      - weight: 100
        podAffinityTerm:
          labelSelector:
            matchLabels:
              app.kubernetes.io/name: praxis-gateway
          topologyKey: kubernetes.io/hostname
```

### Security Context

The charts enforce restricted security defaults. To set a fixed UID
without weakening isolation:

```yaml
podSecurityContext:
  runAsUser: 1000
  runAsGroup: 1000
  fsGroup: 1000
```

Do not set `privileged: true`, `allowPrivilegeEscalation: true`, or
add Linux capabilities.

### Provider Credentials

Mount credentials only in the provider gateway:

```yaml
# provider-gateway-overrides.yaml
credentials:
  - name: openai-credential
    mountPath: /etc/praxis/credentials/openai
    optional: false
```

Never add credential mounts to consumer gateway values.

### Private CA Bundles

For private CAs, use `ca_path` in the Praxis configuration (not in
Helm values). In Helm, mount the CA as part of the TLS Secret:

```bash
kubectl create secret generic provider-tls \
  --from-file=tls.crt=provider.crt \
  --from-file=tls.key=provider.key \
  --from-file=ca.crt=ca.crt \
  -n grid-system
```

### NetworkPolicy

The `grid-mock-providers` chart creates a NetworkPolicy that allows
ingress from pods matching `app.kubernetes.io/instance: provider-gateway`
and `app.kubernetes.io/name: grid-operator`. To override the provider
gateway instance label:

```yaml
# grid-mock-providers overrides
networkPolicy:
  providerGateway:
    instanceLabel: my-provider-gateway
```

### ServiceMonitor

```yaml
# operator-overrides.yaml
serviceMonitor:
  enabled: true
  interval: 30s
  labels:
    release: prometheus
```

The metrics port serves TLS on every grid site. On OpenShift the certificate comes
from the service CA. Elsewhere, a site with `grid.id` or enrollment serves its site
identity once enrollment writes it, and the ServiceMonitor trusts the grid CA. To
use your own certificate, set `metrics.tls.existingSecret` and
`serviceMonitor.tlsConfig`. `metrics.tls.enabled: false` serves plaintext.

A site with `grid.id` and no enrollment whose operator created its own identity
before this release holds a certificate named after the network, not the site, so
its ServiceMonitor scrape fails on the hostname. Delete both `grid-ca` and
`grid-site-identity` to have the operator issue them again under the site name.
That creates a new CA, so peers that pin this site need its new certificate.

On OpenShift a NetworkPolicy also admits only the user-workload and platform
monitoring namespaces to that port. A Prometheus elsewhere needs its namespace in
`networkPolicy.metricsFrom`.

### CRD Retention

Helm does not remove CRDs on uninstall. To remove them:

```bash
kubectl delete crd gridnetworks.grid.praxis.fast \
  gridsites.grid.praxis.fast \
  inferenceproviders.grid.praxis.fast
```

## Troubleshooting

### Service name does not match operator expectation

**Symptom:** Operator logs show it cannot find the provider gateway
Service.

**Cause:** The provider gateway Service name differs from the operator's
`gateway.serviceName`. A Grid gateway uses its Helm release name by default;
`fullnameOverride` can replace that name.

**Fix:** Set `fullnameOverride: provider-gateway` in the provider
gateway values and `gateway.serviceName: provider-gateway` in the
operator values.

### Overlay ConfigMap not created

**Symptom:** Consumer gateway pod stuck in ContainerCreating; operator
logs: "routing overlay has no candidates; skipping ConfigMap apply".

**Cause:** No InferenceProvider matches any GridSite. The GridSite is
missing the `grid.praxis.fast/provider-site` label that the
InferenceProvider's `siteSelector.matchLabels` requires.

**Fix:** Label the GridSite:
`kubectl label gridsite east2 grid.praxis.fast/provider-site=east2`

### TLS handshake failure between gateways

**Symptom:** Consumer gateway returns HTTP 502; logs show
"certificate not valid for name".

**Cause:** The provider TLS certificate SANs don't include the
hostname used in the consumer's `sni` field. Common when reusing
another site's certificates.

**Fix:** Generate site-specific certificates with the correct SANs.

### Credential token mismatch

**Symptom:** Mock inference rejects requests with 401; credential
Secret contains a trailing newline.

**Cause:** `kubectl create secret --from-file` preserves trailing
newlines. The `credential_inject` filter trims them, but env-var
consumers (like mock-inference) see raw bytes.

**Fix:** Create Secrets with `printf '%s' "$(cat token)" | kubectl
create secret --from-file=token=/dev/stdin`.

### External API returns 403

**Symptom:** Provider gateway connects to external API but receives
HTTP 403 from the CDN.

**Cause:** Missing `authority` field in the load_balancer cluster
config. The gateway sends the internal Service name as the HTTP Host
header instead of the external hostname.

**Fix:** Set `authority: api.openai.com` and `tls.sni: api.openai.com`
in the cluster configuration.

### Invalid `ca_system` field

**Symptom:** Provider gateway CrashLoopBackOff with configuration
parse error.

**Cause:** `ca_system` is not a valid Praxis field.

**Fix:** Remove `ca_system`. Use `verify: true` without a `ca:` block
to use the system trust store for public APIs. Use `ca_path` only for
private CAs.

### Consumer returns 500 for some models

**Symptom:** Mock model works but external model returns HTTP 500;
consumer logs: "unknown cluster".

**Cause:** The consumer config's `provider_hop_clusters` and
`load_balancer.clusters` are missing an entry for the external
provider's InferenceProvider CR name.

**Fix:** Add a cluster entry for every InferenceProvider that might
serve the consumer site.

### Mock inference CrashLoopBackOff

**Symptom:** mock-inference pod restarts with "required arguments not
provided".

**Cause:** Missing `--provider` and `--port` startup arguments.

**Fix:** Set `args: ["--provider", "openai", "--port", "8080"]` in
the mock-inference Deployment.

### InferenceProvider stuck Unavailable

**Symptom:** Both InferenceProviders show `phase: Unavailable`; overlay
has no candidates; consumer gateway returns 503.

**Cause:** A NetworkPolicy blocks the AGN Operator from reaching the
mock backend health endpoint. The operator must probe each
InferenceProvider's `spec.endpoint` — if the health check fails, the
provider stays Unavailable and is excluded from the overlay.

**Fix:** Ensure the NetworkPolicy allows ingress from pods with label
`app.kubernetes.io/name: grid-operator` in addition to the provider
gateway. See [NetworkPolicy for Multiple Backends](#networkpolicy-for-multiple-backends).

### Incompatible yq version

**Symptom:** Preflight fails with "unsupported yq implementation".

**Cause:** The Python `yq` wrapper or an older Go `yq` version is
installed instead of Mike Farah's `yq` >= 4.18.0.

**Fix:** Install [mikefarah/yq](https://github.com/mikefarah/yq).

### Context changed accidentally

**Symptom:** Commands affect the wrong cluster.

**Cause:** A prior command modified the default kubectl context.

**Fix:** All scripts use explicit `--kube-context` / `--context`.
Never rely on the default context. Pass `--context` to manual
`kubectl` commands.

## Requirements

- Helm 3.12+
- `grid-operator` image v0.1.1+ (the Helm chart requires `/healthz` and `/readyz`
  health endpoints on the metrics port; v0.1.0 images lack these endpoints and
  will fail liveness probes)
- kubectl configured with contexts for all clusters
- Inter-cluster connectivity between SWIM ports
- TLS certificates and provider credentials prepared out-of-band
- Praxis gateway configuration ConfigMaps created in each cluster
- AGN custom resources applied to each cluster
