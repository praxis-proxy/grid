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
    - grid.cluster-b.example.com:7946
  gatewayRefs:
    - name: inference-gw
      namespace: praxis-system
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
      name: grid-swim-key
      namespace: praxis-system
```

**Phases**: Pending → Initializing → Active → Degraded

**Status fields**: `gridId`, `connectedSites`,
`observedGeneration`, `phase`

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
    strategy: api_key           # see Auth doc
    secretRef:
      name: anthropic-key
      namespace: praxis-system
  accessPolicy:
    siteSelector:
      matchLabels: {}           # empty = all sites
  siteSelector:
    matchLabels: {}
  healthCheck:
    interval: 30s
    path: /v1/messages
```

**Phases**: Pending → Available → Degraded → Unavailable

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
