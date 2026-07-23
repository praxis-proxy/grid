# Authentication & Access Policy

## Authentication layers

Grid-based deployments involve up to three distinct authentication layers.
Each serves a different trust boundary and must not be conflated.

| Layer | What it authenticates | Where it is enforced |
|---|---|---|
| **External caller auth** | The end customer's identity (bearer token, JWT, API key). | At the Praxis edge or consumer gateway, before `grid_route`. |
| **Grid mTLS peer identity** | The edge or consumer gateway's Grid site certificate. | At the provider gateway, via `peer_identity_trust`. |
| **Provider credential injection** | The final-hop gateway's credential for a SaaS/cloud provider API. | At the final-hop gateway, via `grid_credential_inject`. |

The customer's `Authorization` header must not be forwarded as a provider
credential.  Public TLS certificates (for external endpoints) must be kept
separate from Grid site mTLS certificates.

**External caller authentication** is relevant for external client ingress,
where customers outside the cluster reach a public endpoint.  Grid's provider
`accessPolicy` is site-oriented, not tenant-oriented: an edge site's provider
eligibility does not authorize every customer to every model.  Production
external service requires request-time tenant-to-model authorization that is
separate from Grid's site-level access control.  This is not yet implemented.

See [External Client Ingress](external-ingress.md) for the full external
authentication model.

## Provider Authentication Strategies

Authentication in this section means provider authentication: how the final-hop
gateway or provider component that makes the final upstream call authenticates
to the selected backend after routing has chosen a candidate.  It does not
replace or rewrite credentials on the inbound client request.

The implemented native path is `bearer_token`:

1. A provider Secret contains the provider token.
2. `InferenceProvider.spec.auth.secretRef` points at that Secret.
3. The Grid Operator validates the Secret reference.
4. Grid writes only the Secret reference into the routing overlay.
5. The final-hop gateway mounts the Secret as a file.
6. After `grid_route` selects a provider candidate, Praxis AI runs
   `grid_credential_inject`, reads the selected token file, and injects
   `Authorization: Bearer <token>` on the outbound provider request.

Provider tokens are never written into Grid status, routing overlays, or
consumer gateway `ConfigMap`s.

**Implementation status:** the Grid-side contract is implemented: the operator
validates `secretRef`, projects only the reference into `grid-config.json`, and
can render consumer Praxis config with file-backed credential references.  The
request-time filter is the Praxis AI `grid_credential_inject` filter.  Runtime
deployments must use a Praxis AI image that includes `grid_credential_inject`.

| Strategy | Status | Request-time behavior |
|----------|--------|-----------------------|
| `bearer_token` | Implemented native path | Praxis AI reads a mounted Secret file and injects `Authorization: Bearer <token>` on the outbound provider request. |
| `api_key` | Extension point | Static Secret-backed header injection when implemented. |
| `custom` | Extension point | User-configured Secret-backed injection when implemented. |
| `service_account` | Extension point | Kubernetes service-account token injection when implemented. |
| `sigv4` | Extension point | Per-request signing when implemented. |
| `oauth2` | Extension point | Refresh-on-expiry token handling when implemented. |
| `mtls_only` | Extension point | No HTTP credential injection; authentication is certificate-based. |

## Implemented request path

The request path is:

1. Users or external secret managers create provider credentials.
2. Kubernetes Secrets store those credentials.
3. `InferenceProvider.spec.auth.secretRef` points at the Secret.
4. The Grid Operator validates the Secret and projects only the credential
   reference into the routing overlay.
5. The final-hop gateway config maps that reference to a mounted Secret file
   at the deployment point allowed to call the backend.
6. Praxis AI injects the provider credential at request time after `grid_route`
   selects the credential-bearing candidate.

Credential placement follows the final-hop rule:

| Route shape | Where the credential lives | Where injection happens |
|---|---|---|
| Direct API or cloud fallback from the consumer gateway | Secret mounted into that consumer/final-hop gateway pod | The same gateway injects or signs before calling the provider API. |
| Remote Grid site reached over gateway-to-gateway mTLS | Secret mounted only in the remote provider site or provider-side component | The provider-side final-hop component injects before calling its local backend, if that backend needs a provider credential. |
| Local self-hosted backend with no provider API credential | No provider token required | No HTTP credential injection; mTLS or local network policy handles gateway/backend trust. |

In this document, **consumer gateway** (or **ingress gateway**) means the Praxis
gateway receiving the workload request.  **Final-hop gateway** means the Praxis
gateway or provider-side component that makes the final outbound call to the
backend.  For direct API-provider or cloud-provider fallback, the consumer
gateway is often also the final-hop gateway.

### Controller behavior

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
prove the data-plane side for the direct API-provider fallback path:

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

Both paths prove the operator-to-overlay-to-gateway routing chain for a direct
API-provider route.  The native path is the target architecture; static header
injection is kept for regression comparison while the xtask bridge still exists.

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

### Secret placement and production responsibilities

The native injection path keeps credential bytes out of Grid resources and
consumer gateway `ConfigMap`s. Production deployments still need explicit
ownership for credential Secret placement and rotation:

- **Final-hop Secret lifecycle**: the token lives in a Kubernetes Secret mounted
  into the final-hop gateway or provider-side component that is authorized to
  make the final backend call.  The Secret can be created by users, platform
  automation, or an external secret manager.
- **Operator-owned consumer config generation**: `GatewayRef.consumerConfig`
  can render the consumer Praxis `ConfigMap` from routing overlay data,
  including `grid_credential_inject` file references for direct API-provider
  routes.
- **Cross-cluster delivery**: Grid does not copy Secrets across clusters.
  GitOps, External Secrets, Vault, or another platform mechanism must place the
  Secret in the cluster where the final-hop component runs.

See [Consumer Config](consumer-config.md) for the current operator-generated
config shape.

**Additional credential backends** can implement `CredentialResolver` without
changing callers:
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
credential value is mounted into the final-hop gateway or
provider-side component as a Kubernetes Secret file.  `grid_credential_inject`
reads that file at filter construction time and injects
`Authorization: Bearer <token>` after `grid_route`
selects a credential-bearing candidate.

The current tested Praxis AI `grid_credential_inject` implementation uses
read-once/cache behavior: the mounted Secret file is read once during filter
construction, the `Authorization` value is stored in an in-memory `HashMap`, and
per-request injection is a metadata lookup plus header injection.  There is no
Kubernetes API call and no per-request file read.  Secret rotation requires a
Praxis AI config reload or pod restart; automatic rotation is not yet supported.

Static `api_key` and `custom` strategies use the same file-backed injection
seam when implemented.

Dynamic strategies (`sigv4`, `oauth2`) are extension points and need explicit
ownership decisions before implementation:

- `sigv4`: per-request signing by Praxis or a provider adapter using AWS
  credentials from a Secret.
- `oauth2`: refresh-on-expiry token handling by the operator, gateway, or an
  external credential manager.

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

## SWIM Transport Authentication

SWIM gossip carries membership packets, gateway address broadcasts, public
certificate PEM broadcasts, and CRDT provider state.  When
`GridNetwork.spec.tls.swimKeyRef` is configured and the referenced Secret
resolves to a valid 32-byte key, the Grid operator applies the key before
announcing CRD seeds or publishing certificate/provider state for that
`GridNetwork`.  Authenticated SWIM traffic uses AES-256-GCM.  Incoming packets
that do not authenticate are silently dropped before reaching the membership
state machine.

**Secret contract:** `swimKeyRef` points to a Kubernetes Secret in a specified
namespace.  The Secret must contain a key named `"key"` (or the value of
`swimKeyRef.key` if set) with exactly 32 bytes of key material.  The key is
loaded at `GridNetwork` reconcile time.

```yaml
spec:
  tls:
    swimKeyRef:
      name: grid-swim-key
      namespace: praxis-system
      key: key          # default when absent
```

**Configured-key behavior:** when `swimKeyRef` is configured but the Secret is
missing, unreadable, or contains a key of the wrong length, the reconcile fails
before CRD seed announcement and certificate/provider broadcasts.  The operator
does not silently degrade that configured reconcile to plaintext.  Because the
SWIM runtime is process-global, a key loaded by an earlier successful reconcile
remains active until restart.

**Environment variable path:** for local development and Kind-based
testing, set `GRID_SWIM_ENCRYPT_KEY` (a 64-character lowercase hex string
representing 32 bytes) on the operator process.  This takes effect at startup
before the UDP socket processes packets, but environment variables are visible
to same-host process inspectors.  Use Kubernetes Secret references for the
production configuration path.

**Startup plaintext window:** when the operator process starts, the SWIM UDP
socket begins receiving immediately.  If only `swimKeyRef` is configured (no
`GRID_SWIM_ENCRYPT_KEY` env var), the runtime has no key until the first
`GridNetwork` reconcile loads it from the Secret.  During this window — typically
a few seconds — the SWIM socket accepts plaintext packets.  The env var path
closes this window at startup because the key is loaded before the UDP socket
begins processing.  This is a known limitation of the CRD-only key path.

**What SWIM encryption protects:** gossip membership messages, gateway address
and public certificate broadcasts, and CRDT provider state.  It does not protect
data-plane request traffic (that is Praxis/Praxis AI's responsibility).

**Key rotation:** changing the key requires an operator restart.  Multi-key
keyring support (allowing zero-downtime rotation) is not yet implemented.

## Grid mTLS Identity

Grid-generated site certificates set
`OrganizationName = "ai-grid"` (see
`certs::DEFAULT_ORGANIZATION`).  Gateway deployments
that enable peer identity trust can match incoming peer certificates on
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

### Authentication vs authorization

Authentication answers: "is this peer really the Grid site or gateway it claims
to be?"  In the data plane, this is handled by mTLS peer identity and certificate
validation.

Authorization answers: "is this authenticated peer allowed to participate in
this Grid or carry this traffic?"  Grid policy and gateway trust configuration
make that decision.

SWIM discovery is neither authentication nor authorization.  A peer discovered
through gossip must not become routable solely because it is alive.  The control
plane can record discovered sites and trust material, but the provider gateway
still enforces peer identity on every request.

### Public certificate exchange

The Grid operator propagates a site's public certificate PEM to peers via SWIM
state broadcasts when the local `GridNetwork` has `spec.tls.siteSecretRef`
configured.  Before storage, the receiving operator runs a structural check:

- Input containing `PRIVATE KEY` markers is discarded and logged at error level.
  Private key material must never enter status fields or SWIM broadcasts.
- Input without a `-----BEGIN CERTIFICATE-----` header is rejected and recorded
  as `TrustMaterialInvalid` in `GridSite.status.reason`.
- Input with a valid `CERTIFICATE` header passes the structural check and is
  stored in `GridSite.status.publicCertPem`.  The `GridSite` controller then
  sets `status.reason` based on trust policy evaluation (e.g., `TrustPolicyMissing`
  if no fingerprint is configured, or `TrustPolicyMismatch` if the fingerprint does
  not match).

This structural check is **not** cryptographic verification.  It does not parse
DER bytes as X.509, check the issuer or validity period, or validate the signature
against a CA.

A non-empty `publicCertPem` with no private-key rejection indicates:
- The remote site shared a PEM with a `CERTIFICATE` header.
- No private-key markers were detected.
- The structural check passed.

`publicCertPem` does **not** indicate:
- The certificate has been chain-verified against a trusted CA.
- The remote site is authenticated or authorized for routing.
- The mTLS handshake has succeeded.

**Trust policy — fingerprint pinning:** The operator supports explicit control-plane trust
verification through `GridSite.spec.trust.certFingerprint`.  When configured, the operator
computes the SHA-256 fingerprint of the received `publicCertPem` and promotes the site from
`Connecting` to `Active` when the fingerprint matches and the TCP probe succeeds.

GridSite Active is a control-plane eligibility signal. It means Grid has enough
site/trust information to consider the site for overlay generation. It does not
currently prove that a Praxis gateway has completed an mTLS handshake, accepted
client identity, loaded the latest routing config, or authorized provider-side
traffic.

```yaml
spec:
  trust:
    certFingerprint: "ab:cd:ef:..."   # sha256 of publicCertPem PEM bytes
```

To compute the fingerprint from the received certificate:

```bash
kubectl get gridsite <name> -o jsonpath='{.status.publicCertPem}' | \
  tr -d '\n' | sha256sum
# Then convert to colon-separated format and patch spec.trust.certFingerprint.
```

When `spec.trust.certFingerprint` is absent, the site remains `Connecting` with
reason `TrustPolicyMissing`, regardless of cert material.  When the fingerprint is
configured but does not match, the reason is `TrustPolicyMismatch`.

X.509 chain verification against a CA is not yet implemented.  The fingerprint
is a direct comparison of the received certificate content — it verifies that the
certificate is exactly the one expected, but does not validate its chain or
issuer.  Obtain and verify the fingerprint out-of-band before configuring it.

**Certificate rotation:** When `spec.trust.certFingerprint` is configured and the
`Active` `GridSite` controller detects that the received `publicCertPem` no longer
matches the fingerprint, the site is demoted from `Active` to `Connecting` with
reason `TrustPolicyMismatch`.  Update `spec.trust.certFingerprint` to the new
certificate's fingerprint to re-authorize the site.  Until the policy is updated,
the site remains `Connecting` and its CRDT providers are excluded from routing.

Private keys are never broadcast.  The operator reads only the `tls.crt` key from
the site certificate Secret — the `tls.key` key is never accessed for broadcast
purposes.  The gateway enforces mTLS identity on every request independently of
the control-plane `publicCertPem` field.

**Routing eligibility:** Remote CRDT provider records are included in the routing overlay
only when the source `GridSite.status.phase == Active`.  Records from peers in any other
phase (`Discovered`, `Connecting`, `Unreachable`, or missing) are excluded at the
control-plane overlay level.

Active phase indicates the control plane has verified the remote site's certificate
fingerprint and TCP connectivity. Data-plane readiness requires additional steps:
provider gateway mTLS handshake verification, client certificate validation, routing
configuration propagation, and provider-side authorization. These readiness conditions
are enforced at request time by the data-plane gateway filters, not by the control-plane
Active status.

## Separation of Concerns

| Who | What |
|-----|------|
| **Grid Operator** | Validates provider credential `secretRef`; projects credential references (never token values) into routing overlays; can render opt-in consumer Praxis `ConfigMap`; generates local CA and site cert Secrets; marks `GridSite.status.phase = Active` when fingerprint trust policy is satisfied (control-plane eligibility only). |
| **Gateway filters** | `grid_route` selects candidates and writes credential metadata; `grid_credential_inject` reads a mounted Secret file and injects credentials per request; `peer_identity_trust` verifies peer certificate identity on provider gateways. |
| **Deployment / platform** | Provisions gateway trust material (CA cert or cert bundle) at the path referenced by the consumer config's `ca_path`; distributes the Grid CA cert to remote clusters where gateways need to verify peer identity; configures the provider gateway's peer identity filter; manages gateway rollout when trust material changes. |
| **Workload** | Sends requests to the Gateway, optionally with routing headers — never handles provider credentials. |

`Active` GridSite status is the control-plane eligibility gate: it controls whether a remote
site's providers appear in the routing overlay. Active means the control plane has enough
trust information to include the site in routing decisions.

Secure data-plane traffic readiness requires additional steps beyond Active status: gateway
trust material provisioning (CA cert or cert bundle), peer identity filter configuration,
routing configuration loading, and provider authorization. These are deployment prerequisites
and runtime readiness checks, not automatic outputs of Grid's fingerprint verification.
