# Authentication & Access Policy

## Authentication Strategies

Each provider declares how consumers authenticate.
Praxis performs request-time credential injection from gateway configuration.
The intended Grid Operator integration is to read provider credential references
from Kubernetes Secrets and render the corresponding Praxis configuration.

| Strategy | Header | Lifecycle | Used By |
|----------|--------|-----------|---------|
| `bearer_token` | `Authorization: Bearer X` | Static from Secret | OpenAI, Mistral |
| `api_key` | Custom (e.g. `x-api-key`) | Static from Secret | Anthropic |
| `sigv4` | `Authorization: AWS4-HMAC-SHA256...` | Per-request compute *(planned)* | Bedrock |
| `oauth2` | `Authorization: Bearer <token>` | Refresh on expiry *(planned)* | Vertex, Azure |
| `mtls_only` | None (cert-based) | Grid cert lifecycle | Grid-internal |
| `service_account` | `Authorization: Bearer <SA>` | K8s SA token | In-cluster |
| `custom` | User-configured | Static from Secret | Fallback |

> **Implementation note:** Static strategies (`bearer_token`, `api_key`, `custom`,
> `service_account`) are current.  `sigv4` per-request signature computation and
> `oauth2` token refresh are planned and not yet wired into the operator or
> Praxis filter pipeline.

## Current implementation boundary

Grid's desired ownership model is:

1. Users or external secret managers create provider credentials.
2. Kubernetes Secrets store those credentials.
3. `InferenceProvider.spec.auth.secretRef` points at the Secret.
4. The Grid Operator validates and projects the credential into consumer
   Praxis gateway configuration.
5. Praxis injects the provider credential at request time.

### What the controller now owns (controller-owned credential validation, resolver groundwork, and reference projection)

The `InferenceProvider` controller validates credentials during every reconcile:

- Parses `spec.auth` strategy — unsupported strategies immediately drive the
  provider phase to `Unavailable`.
- Validates `spec.auth.secretRef` shape — blank or missing fields drive
  `Unavailable` before any API call.
- Verifies the referenced Kubernetes Secret exists, contains the declared key,
  and the key value is valid UTF-8.
- All credential failures surface in `status.reason` as one of:
  `UnsupportedAuthStrategy`, `CredentialSecretRefInvalid`,
  `CredentialSecretMissing`, `CredentialSecretKeyMissing`,
  `CredentialSecretValueInvalid`.
- `BearerToken` is an opaque type whose `Debug` output is redacted; operator
  resources store only credential references, never token values.
- The `CredentialResolver` trait and `KubernetesSecretResolver` v1 backend are
  in production operator code.
- **Credential reference projection into the routing overlay**: when a provider's
  `spec.auth` declares `strategy: bearer_token` with a valid `secretRef`, the
  operator includes a `credential` field in every routing candidate produced for
  that provider. The field carries `{ strategy, secretRef: { name, namespace, key } }` —
  only the Secret reference, never the token value. This appears in the
  operator-produced `grid-config.json` ConfigMap.

The xtask `verify-api-fallback` and `verify-full-grid-routing` test suites prove
the data-plane side: xtask reads the credential reference for the target API
provider from the operator overlay (not from the provider spec directly), resolves
the token from the referenced Secret, and injects it into the consumer Praxis
config. This harness-generated Praxis config still contains the resolved token;
native Praxis Secret-ref consumption is the remaining production step that keeps
tokens out of data-plane config entirely.

### What remains to complete full controller-owned projection

The operator now projects credential **references** into the routing overlay
(`grid-config.json` ConfigMap).  The remaining gap to full native projection:

- **Praxis native Secret-ref consumption**: the `grid_route` or `headers` filter
  reads the `credential.secretRef` from the overlay candidate and fetches the
  token from Kubernetes at request time — no token in ConfigMaps or Praxis config.
  Until this lands, the xtask harness bridges the gap by reading the credential
  reference from the overlay and resolving the token from the Secret.
- **Operator-owned consumer Praxis config generation**: the operator would own the
  consumer-side Praxis ConfigMap (currently xtask-generated), embedding credential
  references rather than tokens.

**Future credential backends** (implement `CredentialResolver` without changing callers):
- Vault / External Secrets Operator
- OAuth2 token refresh
- SigV4 per-request signing
- Kubernetes workload identity (`ServiceAccount` tokens)

### Manual Override

Any provider can set `auth.manual: true`. When
enabled, the operator does not inject credentials
and the user manages authentication externally.

### Credential Lifecycle

For static strategies (`bearer_token`, `api_key`,
`custom`), the credential value is read from a
Kubernetes Secret at config generation time.

For dynamic strategies (`sigv4`, `oauth2`), the Grid
Operator will manage the credential lifecycle once
these are implemented (currently planned):
- `sigv4`: SigV4 signature computed per-request by
  Praxis using AWS credentials from a Secret *(planned)*
- `oauth2`: Token refreshed before expiry by the
  operator, cached, and injected by Praxis *(planned)*

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

## Grid mTLS Identity

Grid-generated site certificates set
`OrganizationName = "ai-grid"` (see
`certs::DEFAULT_ORGANIZATION`).  Once the AI/Praxis
image includes `peer_identity_trust`, provider
gateways can match incoming peer certificates on
`organization: ai-grid` by default.

Any certificate signed by the Grid CA but with a
different organization value will pass TLS handshake
and fail at the filter, producing an HTTP 403.  This
is the intended fail-closed behaviour for cert-based
bootstrap authentication.

Production deployments should switch to cert-digest
pinning (`cert_digest` field on `trusted_peers`) once
cert identities are stable, as organization matching
is weaker — any cert signed by a trusted CA with the
correct `O=` value is accepted.

## Separation of Concerns

| Who | What |
|-----|------|
| **Grid Operator** | References and validates Kubernetes Secrets. Rendering consumer-side Praxis auth configuration is planned. |
| **Praxis** | Reads gateway config and injects credentials per request through the filter pipeline. |
| **Workload** | Sends requests to the Gateway, optionally with routing headers — never handles provider credentials |
