# Architecture Overview

The AI Grid is a decentralized, peer-to-peer network
for AI inference routing and agentic networking across
clusters, cloud providers, and third-party APIs.

## Two Components

### Grid Operator (this repository)

A Kubernetes controller that orchestrates the mesh.
It does NOT proxy traffic.

Responsibilities:
- **Mesh formation**: SWIM membership via `foca` for
  peer-to-peer discovery
- **Trust**: CA generation, site certificate lifecycle,
  mTLS certificate exchange, trust bundle distribution
- **State propagation**: Delta CRDTs (capabilities,
  metrics, budget) piggybacked on SWIM probes
- **Routing config**: Generates Praxis overlay config
  (clusters, scoring filter, auth injection) as a
  Kubernetes `ConfigMap`

### Praxis AI Gateway (data plane)

The AI-enabled Praxis proxy (`../ai/`) handles all
traffic. It is configured by the Grid Operator.

Responsibilities:
- Request proxying, connection pooling, retries
- API format translation (OpenAI, Anthropic, Bedrock,
  Vertex) via `praxis-ai-apis`
- Credential injection via filter pipeline
- Filter chain execution (guardrails, routing, LB)
- TLS termination and mTLS handshake
- Health checks against local backends
- Hot config reload from overlay `ConfigMap`

### What the Grid Operator does NOT do

- Proxy HTTP traffic
- Translate API formats
- Inject credentials at request time
- Run the filter pipeline

## Workspace Crates

| Crate | Purpose |
|-------|---------|
| `scoring` | Scoring engine, backend types, grid state |
| `certs` | CA and site cert generation, provider trait |
| `crdt` | Delta CRDTs: LWW Register, OR-Set, G-Counter |
| `operator` | K8s controllers, CRDs, operator binary |
| `swim` | SWIM membership wrapper around `foca` |
| `mock-providers` | Mock servers for OpenAI, Anthropic, Bedrock, Vertex |
| `xtask` | Dev task runner for multi-cluster test environments |

Dependency graph:

```text
operator ──→ scoring (scoring)
              ──→ certs (TLS)
              ──→ swim (SWIM runtime)
              ──→ crdt (state propagation)

swim ──→ foca (SWIM protocol)

crdt, scoring, certs: standalone
```

## Backend Categories

Three categories of inference backends:

**Self-hosted clusters**: Running llm-d (initially
supported backend). Full Prometheus metrics. Gateway
selects the cluster; llm-d selects the pod.

**Cloud-managed services**: AWS Bedrock (SigV4),
Google Vertex AI (OAuth2), Azure OpenAI (AAD).
Cloud-specific auth, API formats, and billing.

**Third-party APIs**: OpenAI, Anthropic, Mistral.
Static API keys or bearer tokens. Health derived
from response headers (rate limit remaining,
retry-after).

## Three Provider Types

Each site can offer one or more capability types:

- **InferenceProvider**: Model inference (chat
  completions, embeddings)
- **AgentToolProvider**: MCP tool servers for agent
  tool access
- **AgentToAgentProvider**: A2A agents for
  agent-to-agent delegation

## Integration with Praxis

The Grid Operator and the existing Praxis Operator
both configure Praxis, but own different concerns:

| Concern | Owner |
|---------|-------|
| Base Praxis config (listeners, HTTPRoute chains) | Praxis Operator |
| Grid overlay (clusters, scoring, mTLS) | Grid Operator |
| Praxis Deployment spec | Praxis Operator |
| Overlay volume mount | Grid Operator (SSA) |
| Grid TLS Secrets | Grid Operator |

The Grid Operator patches the Gateway with annotation
`grid.praxis-proxy.io/overlay-config` which the
Praxis Operator reads during reconciliation to
include the overlay `ConfigMap`.
