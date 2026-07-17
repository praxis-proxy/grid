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
`observedGeneration`, `phase`, `consumerConfigStatus[]`

`distributedProviderCount` reflects the number of remote `InferenceProvider`
records received from peer sites via CRDT broadcast.  Local providers and records
from other `GridNetwork`s are excluded from the count.

`consumerConfigStatus[]` is populated for each gateway with
`consumerConfig.enabled: true`, reporting the outcome of the most recent
render/apply attempt.

| Field | Type | Meaning |
|---|---|---|
| `gatewayName` | string | Name of the gateway reference |
| `namespace` | string | Namespace of the gateway and generated `ConfigMap` |
| `configMapName` | string | Name of the generated `ConfigMap` |
| `phase` | enum | `Rendered` \| `Error` \| `Disabled` |
| `reason` | string | Machine-readable reason (`MissingClusterEndpoint`, `ConsumerConfigRenderFailed`, `ConsumerConfigApplyFailed`) — empty when `Rendered` |
| `message` | string | Human-readable diagnostic; never contains token bytes |
| `observedGeneration` | integer | `GridNetwork` generation when this entry was last updated |

Example status output:

```yaml
status:
  phase: Active
  gridId: grid-abc123
  connectedSites: 2
  consumerConfigStatus:
    - gatewayName: inference-gw
      namespace: praxis-system
      configMapName: praxis-consumer-config
      phase: Rendered
      reason: ""
      message: "consumer config rendered and applied to praxis-system/praxis-consumer-config"
      observedGeneration: 7
    - gatewayName: fallback-gw
      namespace: default
      configMapName: op-e2e-consumer-config
      phase: Error
      reason: ConsumerConfigRenderFailed
      message: "consumer config render: overlay local_site must not be blank"
      observedGeneration: 7
```

### CRD-driven SWIM seeds

`spec.seeds` is a list of socket addresses (`host:port`) used to bootstrap
SWIM mesh formation.  Seeds are announced to the running SWIM runtime on every
`GridNetwork` reconcile.  Re-announcing to an existing peer is idempotent — foca
ignores redundant joins.

**Runtime update behavior:**

| Change | Effect |
|---|---|
| Seed added to `spec.seeds` | Announced to the SWIM runtime on the next reconcile (~300 s default); SWIM join initiated |
| Seed removed from `spec.seeds` | No active disconnect; SWIM failure detection ages out the peer naturally |
| `spec.seeds` unchanged | Re-announced on every reconcile; idempotent, no side effects |

**Channel-full behavior:** If the SWIM announce channel is temporarily full
(capacity 16 batches), the announce is silently retried on the next reconcile.
Seeds are not guaranteed to be joined within one reconcile cycle under heavy
broadcast load, but the retry is automatic.

**Scope:** `spec.seeds` targets the SWIM site-membership layer.  The SWIM runtime
is process-global — all `GridNetwork` resources in the operator process share the
same SWIM node.  Seeds from any `GridNetwork` reach the shared SWIM membership
table.  Provider CRDT state remains scoped per network.

**Self-filtering:** The operator removes its own SWIM bind address from
`spec.seeds` before announcing, preventing self-join loops.

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
  - `load_balancer` entries (one per unique candidate cluster). Every referenced
    cluster must have a matching `clusterEndpoints[]` entry with endpoint and
    optional TLS settings
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

**Phases**: Pending → Discovered → Connecting → Active → Unreachable → Left

**Status fields**: `phase`, `reason`, `message`, `observedGeneration`,
`publicCertPem`, `capabilities` (inference, agentTools, agentToAgent),
`lastProbeTime`, `lastTransitionTime`

### GridSite lifecycle

SWIM discovery, authentication, and authorization are separate concerns:

- SWIM discovery identifies a peer and records liveness.
- Authentication proves the peer gateway identity, normally through mTLS
  certificate validation.
- Authorization decides whether that authenticated peer is allowed to
  participate in the Grid or carry traffic for a given policy boundary.

A discovered SWIM peer is not automatically authorized for routing.

| Phase | How entered | Transition driver |
|---|---|---|
| `Pending` | Resource created (manually or by auto-discovery) | Initial default |
| `Discovered` | SWIM peer observed as Alive | `GridNetwork` controller writes on first observation |
| `Connecting` | Gateway address known (`spec.egress.address` non-empty) | `GridSite` controller advances from Discovered; performs TCP probe |
| `Active` | Set by deployment workflow after trust requirements are satisfied | `GridSite` controller preserves Active while probe succeeds |
| `Unreachable` | Probe failure while Active | `GridSite` controller moves Active → Unreachable on TCP probe failure |
| `Left` | Set on graceful site departure | Preserved by operator once set |

**Reason codes** (in `status.reason`):

| Reason | Phase | Meaning |
|---|---|---|
| `AwaitingDiscovery` | Pending | Site record exists; SWIM has not yet observed the peer as Alive |
| `SWIMDiscovered` | Discovered | Peer observed as Alive in SWIM membership; gateway address propagating |
| `GatewayAddressKnown` | Connecting | Gateway address received; advancing to Connecting |
| `GatewayAddressMissing` | Discovered or Connecting | No gateway address known; see `GRID_GATEWAY_ADDRESS` |
| `GatewayReachable` | Connecting | TCP probe to gateway address succeeded; mTLS trust verification is outside this check |
| `GatewayUnreachable` | Connecting or Unreachable | TCP probe to gateway address failed |

**GridSite phase transitions:**

- Pending → Discovered: the `GridNetwork` controller writes `Discovered` when a remote SWIM
  peer is first observed as Alive (requires `grid.praxis-proxy.io/auto-discover-sites: "true"`
  label on the `GridNetwork`).
- Discovered → Connecting: the `GridSite` controller advances automatically when
  `spec.egress.address` is non-empty. For auto-discovered sites, the egress address comes from
  the remote operator's `GRID_GATEWAY_ADDRESS` env var, propagated via SWIM state broadcast.
  If the remote operator has not configured `GRID_GATEWAY_ADDRESS`, the egress address is empty
  and the site stays Discovered with reason `GatewayAddressMissing`.
- Connecting: the `GridSite` controller runs a TCP probe against `spec.egress.address` on each
  reconcile. The probe result is reflected in `reason` (`GatewayReachable` / `GatewayUnreachable`).
  This probe only proves TCP reachability. Advancing to Active requires mTLS trust
  verification and authorization, which are outside the current TCP probe scope.
  Active must be set by the deployment workflow once trust requirements are satisfied.
- Active → Unreachable: the `GridSite` controller demotes Active to Unreachable when the TCP
  probe fails, allowing the overlay to deprioritize unreachable sites.

**`spec.egress.address` source:** For auto-discovered sites, the egress address is sourced from
the remote operator's `GRID_GATEWAY_ADDRESS` environment variable, propagated through the SWIM
state broadcast.  If the remote operator has not configured `GRID_GATEWAY_ADDRESS`, the field
is empty and the site stays Discovered.  For manually-applied `GridSite` resources, set
`spec.egress.address` explicitly to the data-plane gateway endpoint.

**`status.publicCertPem`:** The public site certificate PEM received from the remote site via
SWIM state broadcast.  Before storage, the operator performs a structural check:
private-key markers (`PRIVATE KEY`) cause the input to be discarded entirely and an error
logged.  Non-certificate PEM triggers `TrustMaterialInvalid` status.  A valid `CERTIFICATE`
header passes the structural check.

This field contains only the public certificate — never a private key.  A non-empty
`publicCertPem` means the remote site has shared its public identity material and the
structural check passed.  It does **not** mean:

- The certificate has been chain-verified against a trusted CA.
- The peer is authenticated or authorized for routing.
- The content has been parsed as X.509.

Private keys, bearer tokens, provider credentials, and Kubernetes Secret contents must never
be written to status.

**`status.reason` values for `Connecting` phase:**

| Reason | Meaning |
|---|---|
| `GatewayAddressKnown` | Egress address received; site advancing from Discovered to Connecting |
| `GatewayUnreachable` | TCP probe to gateway address failed |
| `GatewayAddressMissing` | No gateway address set; cannot probe |
| `TrustMaterialPresent` | TCP probe succeeded; `publicCertPem` set and structurally valid (not chain-verified) |
| `TrustMaterialMissing` | TCP probe succeeded; no public certificate received yet |
| `TrustMaterialInvalid` | Received PEM failed structural check (not a certificate, or discarded private key) |

**Routing eligibility:** `GridSite.status.phase == Active` is the control-plane gate for
remote CRDT provider records.  Provider records from a peer whose `GridSite` is in
`Discovered`, `Connecting`, `Pending`, `Unreachable`, or `Left` are excluded from the
routing overlay.  Records from a peer with no matching `GridSite` are also excluded
(fail-closed).  Trust enforcement at request time is additionally enforced by data-plane
mTLS at the provider gateway.  See [Routing eligibility](routing.md#routing-eligibility)
for the full gating rule.

Example status — gateway reachable, valid cert received:

```yaml
status:
  phase: Connecting
  reason: TrustMaterialPresent
  message: >
    gateway reachable; public certificate PEM received and structurally valid;
    certificate has not been chain-verified — Active requires explicit trust policy
  observedGeneration: 3
```

Example status — gateway reachable, no cert yet:

```yaml
status:
  phase: Connecting
  reason: TrustMaterialMissing
  message: "gateway reachable; awaiting public trust material from remote site"
  observedGeneration: 3
```

Example status — gateway address not configured on remote operator:

```yaml
status:
  phase: Discovered
  reason: GatewayAddressMissing
  message: "gateway address not yet available; cannot advance to Connecting"
  observedGeneration: 2
```

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
