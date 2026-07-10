# Authentication & Access Policy

## Authentication Strategies

Each provider declares how consumers authenticate.
The Grid Operator manages credential lifecycle and
configures Praxis to inject credentials transparently.

| Strategy | Header | Lifecycle | Used By |
|----------|--------|-----------|---------|
| `bearer_token` | `Authorization: Bearer X` | Static from Secret | OpenAI, Mistral |
| `api_key` | Custom (e.g. `x-api-key`) | Static from Secret | Anthropic |
| `sigv4` | `Authorization: AWS4-HMAC-SHA256...` | Per-request compute | Bedrock |
| `oauth2` | `Authorization: Bearer <token>` | Refresh on expiry | Vertex, Azure |
| `mtls_only` | None (cert-based) | Grid cert lifecycle | Grid-internal |
| `service_account` | `Authorization: Bearer <SA>` | K8s SA token | In-cluster |
| `custom` | User-configured | Static from Secret | Fallback |

### Manual Override

Any provider can set `auth.manual: true`. When
enabled, the operator does not inject credentials
and the user manages authentication externally.

### Credential Lifecycle

For static strategies (`bearer_token`, `api_key`,
`custom`), the credential value is read from a
Kubernetes Secret at config generation time.

For dynamic strategies (`sigv4`, `oauth2`), the Grid
Operator manages the credential lifecycle:
- `sigv4`: SigV4 signature computed per-request by
  Praxis using AWS credentials from a Secret
- `oauth2`: Token refreshed before expiry by the
  operator, cached, and injected by Praxis

## Access Policy

Two layers of access control:

### Network Policy (site-to-site)

Defined on `GridNetwork`. Controls which sites can
establish data-plane connections at all. Default:
all sites in the same `GridNetwork` can connect.

```yaml
spec:
  networkPolicy:
    defaultAllow: true
    deny:
      - site: untrusted-partner
```

### Provider Access Policy (per-provider)

Defined on each provider CRD via `accessPolicy`.
Controls which sites can consume this provider.

```yaml
spec:
  accessPolicy:
    siteSelector:
      matchLabels:
        grid.praxis-proxy.io/site: cluster-a
```

Empty `matchLabels` = all sites in the grid.

## Workload Access Patterns

How workloads discover and consume grid providers:

### 1. SNI-based (default)

Workloads send requests to well-known DNS names:

```text
inference.grid.local        → inference routing
claude-sonnet-4.grid.local  → model-specific
tools.grid.local            → MCP tool federation
agents.grid.local           → A2A agent routing
```

The Gateway uses SNI to identify grid traffic and
applies the grid scoring filter.

### 2. Header-based

Routing headers on requests to the Gateway:

```text
X-Grid-Model: claude-sonnet-4
X-Grid-Capability: tool_calling
```

### 3. OpenAI-compatible

Standard `POST /v1/chat/completions` with a `grid/`
model prefix:

```json
{"model": "grid/claude-sonnet-4", "messages": [...]}
```

### 4. MCP Discovery

Connect to the Gateway's MCP endpoint:
- `tools/list` → federated tool inventory
- `tools/call` → routed to hosting site

### 5. A2A Discovery

- `GET /.well-known/agent.json` → aggregated Agent
  Cards
- A2A `SendMessage` → capability-based routing

### 6. Provider Discovery API

```text
GET /v1/grid/providers
```

Returns all accessible providers filtered by the
workload's identity and access policies.

## Separation of Concerns

| Who | What |
|-----|------|
| **Grid Operator** | Manages credentials (Secrets), generates overlay config with auth strategy per cluster, rotates tokens |
| **Praxis** | Reads overlay config, injects credentials per-request via filter pipeline, strips client-supplied auth |
| **Workload** | Sends requests to the Gateway, optionally with routing headers — never handles provider credentials |
