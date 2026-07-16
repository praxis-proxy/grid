# Authentication & Access Policy

## Authentication Strategies

Each provider declares how consumers authenticate.
Praxis performs request-time credential injection from gateway configuration.
Grid's implemented bearer-token path projects credential references into the
routing overlay, then uses an AI-side `grid_credential_inject` filter to read
the selected token from a mounted Kubernetes Secret file.

| Strategy | Header | Lifecycle | Used By |
|----------|--------|-----------|---------|
| `bearer_token` | `Authorization: Bearer X` | Static from mounted Secret file | OpenAI, Mistral |
| `api_key` | Custom (e.g. `x-api-key`) | Static from Secret *(planned)* | Anthropic |
| `sigv4` | `Authorization: AWS4-HMAC-SHA256...` | Per-request compute *(planned)* | Bedrock |
| `oauth2` | `Authorization: Bearer <token>` | Refresh on expiry *(planned)* | Vertex, Azure |
| `mtls_only` | None (cert-based) | Grid cert lifecycle | Grid-internal |
| `service_account` | `Authorization: Bearer <SA>` | K8s SA token *(planned)* | In-cluster |
| `custom` | User-configured | Static from Secret *(planned)* | Fallback |

> **Implementation note:** `bearer_token` is the current native data-plane path.
> `api_key`, `custom`, `service_account`, SigV4 per-request signing, and OAuth2
> refresh are extension points and are not yet wired into the operator or Praxis
> filter pipeline.

## Current implementation boundary

Grid's desired ownership model is:

1. Users or external secret managers create provider credentials.
2. Kubernetes Secrets store those credentials.
3. `InferenceProvider.spec.auth.secretRef` points at the Secret.
4. The Grid Operator validates the Secret and projects only the credential
   reference into the routing overlay.
5. The consumer gateway config maps that reference to a mounted Secret file.
6. Praxis injects the provider credential at request time after `grid_route`
   selects the credential-bearing candidate.

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

The xtask `verify-api-fallback` and `verify-api-fallback-native` test suites
prove the data-plane side:

- **Static header injection (`verify-api-fallback`)**: xtask reads the
  credential reference from the operator overlay, resolves the token from the
  K8s Secret, and writes it as a static `filter: headers` / `request_set`
  value in the consumer Praxis config. Token appears in the consumer Praxis
  `ConfigMap`.

- **Native path (`verify-api-fallback-native`)**: xtask reads the credential
  reference from the operator overlay, resolves the token, then generates consumer
  config using `grid_route` (with credential `secretRef` in candidates) +
  `grid_credential_inject` filter with a `file:` source pointing at a mounted
  Kubernetes Secret.  The token does not appear in the operator overlay JSON,
  in `grid_route` candidates, or in the consumer Praxis `ConfigMap`.

Both paths prove the operator→overlay→consumer routing chain.  The native path
is the target architecture; static header injection is kept for regression
comparison while the xtask bridge still exists.

### Supplying provider tokens

For both validation paths, the install-time input is the same Kubernetes Secret
plus an `InferenceProvider.spec.auth.secretRef`.  The Secret contains the
provider token; the `InferenceProvider` points at the Secret without copying the
token into Grid resources.

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: api-provider-creds
  namespace: default
type: Opaque
stringData:
  token: sk-provider-token
```

```yaml
apiVersion: inference.networking.x-k8s.io/v1alpha1
kind: InferenceProvider
metadata:
  name: api-provider
spec:
  auth:
    strategy: bearer_token
    secretRef:
      name: api-provider-creds
      namespace: default
      key: token
```

The difference is where the resolved token lands:

- **Static header injection** resolves the Secret during xtask config
  generation and writes `Authorization: Bearer ...` directly into the consumer
  Praxis `ConfigMap`.
- **Native credential injection** mounts the Secret into the consumer pod and
  writes only a `file:` reference into the consumer Praxis `ConfigMap`.

### What remains to complete full native projection

The native injection path is now implemented.  The remaining gap:

- **Consumer-cluster Secret lifecycle**: the token now lives in a Kubernetes
  Secret mounted into the consumer gateway pod.  The xtask harness provisions
  that Secret for validation.  Production needs operator-owned provisioning,
  rotation, and cross-cluster Secret synchronization policy.
- **Operator-owned consumer config generation**: the operator should generate the
  full consumer Praxis ConfigMap (currently xtask-generated), embedding
  `grid_credential_inject` file references from the overlay rather than requiring
  the xtask bridge.

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

For the current static `bearer_token` strategy, the
credential value is mounted into the consumer gateway
pod as a Kubernetes Secret file.  `grid_credential_inject`
reads that file at filter construction time and injects
`Authorization: Bearer <token>` after `grid_route`
selects a credential-bearing candidate.

Static `api_key` and `custom` strategies are planned
extensions of the same file-backed injection seam.

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
| **Grid Operator** | References and validates Kubernetes Secrets; projects credential references into routing overlays. Operator-owned consumer config and Secret lifecycle are planned. |
| **Praxis / AI filters** | `grid_route` selects candidates and writes credential metadata; `grid_credential_inject` reads a mounted Secret file and injects credentials per request. |
| **Workload** | Sends requests to the Gateway, optionally with routing headers — never handles provider credentials |
