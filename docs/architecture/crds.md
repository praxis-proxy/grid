# Custom Resource Definitions

API group: `grid.praxis-proxy.io/v1alpha1`

All CRDs are cluster-scoped.

## GridNetwork

The grid itself. Top-level tenancy boundary. A single
cluster can host multiple `GridNetworks` for
multi-tenancy.

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridNetwork
metadata:
  name: production
spec:
  gridId: ""                    # auto-generated on first join
  seeds:
    - "10.0.0.5:7946"
  gatewayRefs:
    - name: inference-gw
      namespace: praxis-system
      localSiteName: cluster-east   # optional; defaults to network name
      consumerConfig:               # optional; opt-in consumer Praxis config generation
        enabled: true
        credentialMountBase: /run/secrets/grid-credentials
        configMapName: praxis-consumer-config
        tlsCertMountPath: /etc/praxis/tls
        clusterEndpoints:           # optional; endpoint topology for load_balancer
          - cluster: site-a
            address: "10.0.0.4:30080"
            sni: site-a.grid.internal
          - cluster: api-provider
            address: "mock-api.default.svc:8080"
            # sni absent → plain HTTP, no mTLS
  region: us-east-1
  zone: us-east-1a
  swim:
    probeInterval: 5s           # WAN probe interval
    suspicionTimeout: 10s       # before declaring dead
    gossipNodes: 3              # indirect probe fanout
  tls:
    caSecretRef:
      name: grid-ca
      namespace: praxis-system
    siteSecretRef:
      name: grid-site-cert
      namespace: praxis-system
    swimKeyRef:
      name: swim-key
      namespace: praxis-system
```

**Phases**: Pending → Initializing → Active → Degraded

**Status fields**: `gridId`, `connectedSites`, `distributedProviderCount`,
`observedGeneration`, `phase`

`distributedProviderCount` reflects the number of remote `InferenceProvider`
records received from peer sites via CRDT broadcast.  Local providers and records
from other `GridNetwork`s are excluded from the count.

### Stale candidate TTL

`spec.staleCandidateTtlSeconds` controls when stale (fresh=false) remote
candidates are evicted from the rendered overlay.

| Value | Behaviour |
|---|---|
| Absent (default) | Stale candidates are retained indefinitely in the overlay. |
| `0` | Rejected by the CRD schema (`minimum: 1`). |
| `N >= 1` | Remote candidates with SWIM member age `>= N` seconds are omitted from the overlay. |

Local and healthy remote candidates are never evicted.  CRDT storage records
are not deleted by this mechanism.

### GatewayRef.consumerConfig

`spec.gatewayRefs[].consumerConfig` opts a gateway into operator-managed consumer
Praxis `ConfigMap` generation.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Set to `true` to enable consumer config generation for this gateway. |
| `credentialMountBase` | `/run/secrets/grid-credentials` | Base directory where credential Secrets are mounted inside the consumer pod. |
| `configMapName` | `praxis-consumer-config` | Name of the generated `ConfigMap` in the gateway namespace. |
| `clusterEndpoints[]` | `[]` | Optional endpoint topology for `load_balancer` clusters. Each entry maps a candidate cluster name to an address and optional SNI. |
| `tlsCertMountPath` | `/etc/praxis/tls` | Base path for mounted TLS files used when a `clusterEndpoints[]` entry sets `sni`. |
| `listenerPort` | `8080` | HTTP port for the generated `listeners[0].address` (`0.0.0.0:{listenerPort}`). |

When `enabled: true`, the `GridNetwork` controller renders a `praxis.yaml`-keyed
`ConfigMap` in the gateway namespace on each reconcile.  The generated config is a
complete, runnable Praxis config containing:

- `listeners:` — one public listener at `0.0.0.0:{listenerPort}`
- `filter_chains:` — the consumer chain with:
  - `grid_route` candidates from the routing overlay (with `credential.secretRef` for
    credential-bearing candidates)
  - `grid_credential_inject` entries (one per unique credential reference) using
    `file:` sources — token bytes are never written to the `ConfigMap`
  - `load_balancer` entries (one per unique candidate cluster). Clusters with
    matching `clusterEndpoints[]` entries include endpoint and TLS settings;
    clusters without a match render as name-only stubs
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

When `enabled: false` or `consumerConfig` is absent, this gateway behaves as before
— only the routing overlay `ConfigMap` is applied.

## GridSite

Represents another site in the grid. Created manually
for seed peers or automatically by SWIM discovery.

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridSite
metadata:
  name: cluster-b
  labels:
    grid.praxis-proxy.io/network: production
spec:
  gridNetworkRef: production
  egress:
    address: egress.cluster-b.example.com:8443
    tls:
      mode: Mutual
  region: us-east-1
  zone: us-east-1a
  sovereigntyZone: us
```

**Phases**: Pending → Discovered → Connecting →
Active → Unreachable → Left

A GridSite does NOT reach Active until:
1. SWIM connectivity confirmed
2. mTLS certificates exchanged and verified
3. At least one provider capability negotiated
4. Data plane ping successful

**Status fields**: `phase`, `publicCertPem`,
`capabilities` (inference, agentTools, agentToAgent),
`lastProbeTime`, `observedGeneration`

## InferenceProvider

Represents an inference backend available over the
grid.

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: InferenceProvider
metadata:
  name: anthropic-api
spec:
  gridNetworkRef: production
  providerKind: anthropic       # open_ai | anthropic | bedrock | vertex | self_hosted
  backendKind: api_provider     # local | remote | cloud_managed | api_provider
  endpoint: https://api.anthropic.com
  models:
    - name: claude-sonnet-4
      contextWindow: 200000
      capabilities: [tool_calling, vision, streaming]
  cost:
    perMillionInputTokens: 3.0
    perMillionOutputTokens: 15.0
  auth:
    strategy: bearer_token      # current native path; see Auth doc
    secretRef:
      name: anthropic-token
      namespace: praxis-system
      key: token
  accessPolicy:
    siteSelector:
      matchLabels: {}           # empty = all sites
  siteSelector:
    matchLabels: {}
  healthCheck:
    interval: 30s
    path: /v1/messages
  metricsConfig:
    path: /metrics
    timeout: 2s
    signalNames:
      queueDepth: provider_queue_depth_normalized
      kvCacheUtilization: provider_kv_cache_utilization
      latencyP99Ms: provider_latency_p99_ms
      prefixCacheHitRatio: provider_prefix_cache_hit_ratio
      errorRate: provider_error_rate
      healthy: provider_healthy
```

**Phases**: Pending → Available → Degraded → Unavailable

### Backend kind

`spec.backendKind` describes the provider's placement and policy category:

| Value | Meaning |
|-------|---------|
| `local` | Self-hosted capacity in the local site. |
| `remote` | Self-hosted capacity in another Grid site. |
| `cloud_managed` | Managed cloud capacity controlled by the operator's cloud account. |
| `api_provider` | External API/SaaS provider used as fallback or explicit API route. |

The value influences scoring and routing policy. It does not require a specific
transport implementation; for example, a `cloud_managed` backend can still be
fronted by Praxis.

### Credential projection

`spec.auth.secretRef` points to a Kubernetes Secret that contains provider
credential bytes.  For the current native `bearer_token` path:

1. The operator validates that the Secret exists and contains the referenced key.
2. The routing overlay candidate receives only:

   ```json
   {
     "credential": {
       "strategy": "bearer_token",
       "secretRef": {
         "name": "anthropic-token",
         "namespace": "praxis-system",
         "key": "token"
       }
     }
   }
   ```

3. The consumer Praxis config uses `grid_credential_inject` with a `file:` source
   pointing at a mounted Secret file.

Token bytes do not appear in the overlay `ConfigMap`, `grid_route` candidates,
filter metadata, or the consumer Praxis `ConfigMap`.

### Metrics configuration

`spec.metricsConfig` configures the operator-side Prometheus scrape used during
`GridNetwork` reconciliation. When present, the operator scrapes
`{spec.endpoint}{metricsConfig.path}`, parses the configured signal names, and
feeds the resulting `BackendMetrics` into overlay scoring.

| Field | Default | Meaning |
|-------|---------|---------|
| `path` | `/metrics` | HTTP path, relative to `spec.endpoint`. |
| `timeout` | `2s` | Scrape timeout. `s` and `ms` suffixes are recognized. |
| `signalNames` | all unset | Mapping from scoring signals to Prometheus metric names. |
| `staleMetricsSeconds` | absent | Grace period (seconds) for using a cached sample when the current scrape fails.  When absent, scrape failures immediately produce neutral scoring.  Minimum: `1`. |

Providers without `metricsConfig`, providers with failed scrapes (outside any
configured grace period), and signals without configured metric names use neutral
metric scores.  See [Stale metrics grace period](routing.md#stale-metrics-grace-period)
in the routing architecture for the full semantics.

#### Signal names

| Field | Expected value |
|-------|----------------|
| `queueDepth` | Normalized queue depth from `0.0` to `1.0`. |
| `kvCacheUtilization` | KV-cache utilization from `0.0` to `1.0`. |
| `latencyP99Ms` | P99 request latency in milliseconds. |
| `prefixCacheHitRatio` | Prefix-cache hit ratio from `0.0` to `1.0`. |
| `errorRate` | Error rate from `0.0` to `1.0`. |
| `healthy` | Health gauge interpreted by the metrics parser. |

#### Queue depth normalization

Grid does not normalize raw queue counts. Exporters should publish
`queueDepth` as a normalized `0.0`–`1.0` gauge before the operator scrapes it.

## AgentToolProvider

Represents MCP tool servers available over the grid.

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: AgentToolProvider
metadata:
  name: db-tools
spec:
  gridNetworkRef: production
  protocol: mcp
  endpoint: http://db-tools.tools:8080
  tools:
    - name: database-query
      description: "Query the database"
  auth:
    strategy: bearer_token
    secretRef:
      name: tool-token
      namespace: praxis-system
  accessPolicy:
    siteSelector:
      matchLabels:
        grid.praxis-proxy.io/site: cluster-a
```

**Phases**: Pending → Available → Degraded → Unavailable

**Status fields**: `discoveredTools` (auto-populated
from MCP `tools/list`), `matchingSites`

## AgentToAgentProvider

Represents A2A agents available over the grid.

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: AgentToAgentProvider
metadata:
  name: claims-agent
spec:
  gridNetworkRef: production
  protocol: a2a
  endpoint: http://claims-agent.agents:8080
  agentCard:
    skills: [claims-processing, document-review]
    modalities: [text]
  auth:
    strategy: mtls_only
  accessPolicy:
    siteSelector:
      matchLabels:
        grid.praxis-proxy.io/site: cluster-a
```

**Phases**: Pending → Available → Degraded → Unavailable
