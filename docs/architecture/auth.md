# Authentication & Access Policy

## Authentication layers

AI Grid Network (AGN) deployments involve up to three distinct authentication layers.
Each serves a different trust boundary and must not be conflated.

| Layer | What it authenticates | Where it is enforced |
|---|---|---|
| **External caller auth** | The end customer's identity (bearer token, JWT, API key). | At the Praxis edge or consumer gateway, before `intelligent_route`. |
| **AGN mTLS peer identity** | The edge or consumer gateway's AGN site certificate. | At the provider gateway, via `peer_identity_trust`. |
| **Provider credential injection** | The final-hop gateway's credential for a SaaS/cloud provider API. | At the final-hop gateway, via `credential_inject`. |

The customer's `Authorization` header must not be forwarded as a provider
credential.  Public TLS certificates (for external endpoints) must be kept
separate from AGN site mTLS certificates.

**External caller authentication** is relevant for external client ingress,
where customers outside the cluster reach a public endpoint. AGN's provider
`accessPolicy` is site-oriented, not tenant-oriented: an edge site's provider
eligibility does not authorize every customer to every model.  Production
external service requires request-time tenant-to-model authorization that is
separate from AGN's site-level access control. This is not yet implemented.

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
3. The AGN Operator validates the Secret reference.
4. AGN writes only the Secret reference into the routing overlay.
5. The final-hop gateway mounts the Secret as a file.
6. After `intelligent_route` selects a provider candidate, Praxis AI runs
   `credential_inject`, reads the selected token file, and injects
   `Authorization: Bearer <token>` on the outbound provider request.

Provider tokens are never written into AGN status, routing overlays, or
consumer gateway `ConfigMap`s.

**Implementation status:** the AGN-side contract is implemented: the operator
validates `secretRef`, projects only the reference into `routing-config.json`, and
can render consumer Praxis config with file-backed credential references.  The
request-time filter is the Praxis AI `credential_inject` filter.  Runtime
deployments must use a Praxis AI image that includes `credential_inject`.

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
4. The AGN Operator validates the Secret and projects only the credential
   reference into the routing overlay.
5. The final-hop gateway config maps that reference to a mounted Secret file
   at the deployment point allowed to call the backend.
6. Praxis AI injects the provider credential at request time after `intelligent_route`
   selects the credential-bearing candidate.

Credential placement follows the final-hop rule:

| Route shape | Where the credential lives | Where injection happens |
|---|---|---|
| Direct API or cloud fallback from the consumer gateway | Secret mounted into that consumer/final-hop gateway pod | The same gateway injects or signs before calling the provider API. |
| Remote AGN site reached over gateway-to-gateway mTLS | Secret mounted only in the remote provider site or provider-side component | The provider-side final-hop component injects before calling its local backend, if that backend needs a provider credential. |
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
  operator-produced `routing-config.json` ConfigMap.

The xtask `verify-api-fallback` and `verify-api-fallback-native` test suites
prove the data-plane side for the direct API-provider fallback path:

- **Static header injection (`verify-api-fallback`)**: xtask reads the
  credential reference from the operator overlay, resolves the token from the
  K8s Secret, and writes it as a static `filter: headers` / `request_set`
  value in the consumer Praxis config. Token appears in the consumer Praxis
  `ConfigMap`.

- **Native path (`verify-api-fallback-native`)**: xtask reads the credential
  reference from the operator overlay, resolves the token, then generates consumer
  config using `intelligent_route` (with credential `secretRef` in candidates) +
  `credential_inject` filter with a `file:` source pointing at a mounted
  Kubernetes Secret.  The token does not appear in the operator overlay JSON,
  in `intelligent_route` candidates, or in the consumer Praxis `ConfigMap`.

Both paths prove the operator-to-overlay-to-gateway routing chain for a direct
API-provider route.  The native path is the target architecture; static header
injection is kept for regression comparison while the xtask bridge still exists.

### Supplying provider tokens

For both validation paths, the install-time input is the same Kubernetes Secret
plus an `InferenceProvider.spec.auth.secretRef`.  The Secret contains the
provider token; the `InferenceProvider` points at the Secret without copying the
token into AGN resources.

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

Partial `InferenceProvider.spec.auth` snippet (merge it into a complete
`InferenceProvider` using the `grid.praxis.fast/v1alpha1` API):

```yaml
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

The native injection path keeps credential bytes out of AGN resources and
consumer gateway `ConfigMap`s. Production deployments still need explicit
ownership for credential Secret placement and rotation:

- **Final-hop Secret lifecycle**: the token lives in a Kubernetes Secret mounted
  into the final-hop gateway or provider-side component that is authorized to
  make the final backend call.  The Secret can be created by users, platform
  automation, or an external secret manager.
- **Operator-owned consumer config generation**: `GatewayRef.consumerConfig`
  can render the consumer Praxis `ConfigMap` from routing overlay data,
  including `credential_inject` file references for direct API-provider
  routes.
- **Cross-cluster delivery**: AGN does not copy Secrets across clusters.
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
provider-side component as a Kubernetes Secret file.  `credential_inject`
reads that file at filter construction time and injects
`Authorization: Bearer <token>` after `intelligent_route`
selects a credential-bearing candidate.

The current tested Praxis AI `credential_inject` implementation uses
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
        grid.praxis.fast/site: cluster-a
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
resolves to a valid 32-byte key, the AGN Operator applies the key before
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

**Startup hold:** the operator reads the `GridNetwork` key before its first
SWIM send. Until a key loads, SWIM sends and receives nothing. Only a network
that declares no `swimKeyRef` releases the hold to plaintext. The hold also
covers the time before any network exists. Set `GRID_SWIM_REQUIRE_KEY=false`
to skip that part. If the startup list fails, the operator retries it in the
background. A malformed `GRID_SWIM_ENCRYPT_KEY` stops the operator. The
`grid_swim_key_pending` gauge reads 1 while held.

**What SWIM encryption protects:** gossip membership messages, gateway address
and public certificate broadcasts, and CRDT provider state.  It does not protect
data-plane request traffic (that is Praxis/Praxis AI's responsibility).

**Key rotation:** changing the key requires an operator restart.  Multi-key
keyring support (allowing zero-downtime rotation) is not yet implemented.

**What the key does not stop:** any holder of the SWIM key can still mark a
site down or evict it from membership. Identities are not yet signed per site.

## Grid mTLS Identity

Grid-generated site certificates set `OrganizationName` to the site name, which
is also the SPIFFE path segment the certificate carries. A gateway that enables
peer identity trust can therefore match an incoming peer on
`organization: <site>` and tell one consumer site from another.

That matters because the SPIFFE ID is validated during the handshake and is not
carried into the request path, so the organization is the only identity field a
filter can read per request. A certificate signed by the grid CA whose
organization names another site passes the handshake and fails at the filter
with an HTTP 403, which is the intended fail-closed behaviour.

An infrastructure certificate, issued for a database or a serving listener,
carries no organization and no SPIFFE SAN. It names no site on either axis, so
it matches no `organization` entry.

Two properties hold this up. The grid CA constructs every subject itself and
emits exactly one organization value, and `sign_csr` carries only the request's
public key forward, so an enrollee cannot shape the field it is authorized by.
A site name is restricted to a lowercase DNS label, which keeps the organization
and the SPIFFE segment in step.

Pinning `cert_digest` on `trusted_peers` remains available and is stricter still,
at the cost of re-pinning on every reissue.

Site certificates follow the X.509-SVID leaf profile. Each carries a
critical CA:FALSE basic constraint and a critical key usage of digital
signature only. Each also carries server and client authentication, one
SPIFFE URI SAN, and the site's DNS SAN. A provider gateway in SPIFFE mode refuses a leaf without
those extensions. Certificates issued before this profile lack them, so
re-enroll those sites before switching a provider to SPIFFE mode.

SPIFFE mode, `spec.peerTrust.mode: spiffe` on the `GridNetwork`, reads no pins.
It admits any site the Grid CA signed, auto-discovered sites included, and it
cannot revoke one short of rotating the Grid CA and the SWIM key.

On the signals listener, only this site's current leaf, matched by digest,
reads unscoped as the co-located gateway, in either mode. The listener refuses
a reissued leaf for the same site. In SPIFFE mode the operator polls only sites
it holds a `GridSite` for. It also waits for encrypted SWIM gossip, since gossip
carries the addresses it dials. The listener caps handshakes per source address
and per global IPv6 /64 and /48. It names the caller before it counts the
connection. It closes a caller it cannot name right after the handshake. It
caps authenticated connections per named site and answers 503 past that cap.
Peers together never hold the last eight connections, which stay free for the
co-located gateway. The caps start over when the listener reloads its TLS
material. It closes a connection whose response write stalls or that outlives
five minutes. Restrict who can reach the listener with
`loadBalancerSourceRanges` on its Service or with a NetworkPolicy.

The gateway polls peers from the serving config the operator renders. It
always checks the peer's SPIFFE ID. Under pin trust the operator also renders
each peer's declared pins, and the gateway refuses a leaf that matches none.

### Authentication vs authorization

Authentication answers: "is this peer really the AGN site or gateway it claims
to be?"  In the data plane, this is handled by mTLS peer identity and certificate
validation.

Authorization answers: "is this authenticated peer allowed to participate in
this AI Grid Network or carry this traffic?" AGN policy and gateway trust configuration
make that decision.

SWIM discovery is neither authentication nor authorization.  A peer discovered
through gossip must not become routable solely because it is alive.  The control
plane can record discovered sites and trust material, but the provider gateway
still enforces peer identity on every request.

### Public certificate exchange

The AGN Operator propagates a site's public certificate PEM to peers via SWIM
state broadcasts when the local `GridNetwork` has `spec.tls.siteSecretRef`
configured.  Before storage, the receiving operator runs a structural check:

- Input containing `PRIVATE KEY` markers is discarded and logged at error level.
  Private key material must never enter status fields or SWIM broadcasts.
- Input without a `-----BEGIN CERTIFICATE-----` header is rejected and recorded
  as `TrustMaterialInvalid` in `GridSite.status.reason`.
- Input with a valid `CERTIFICATE` header passes the structural check and is
  stored in `GridSite.status.publicCertPem`.

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

**Identity-aware gateway verification:** For `spec.egress.tls.mode: Mutual`,
the operator performs an mTLS handshake with the advertised gateway. It verifies
the server chain against `GridNetwork.spec.tls.caSecretRef`, verifies the DNS SAN
against `spec.egress.tls.serverName`, proves possession of the server private key
through the handshake, and checks the live leaf certificate against
`spec.trust.canonicalFingerprints`.

GridSite Active is a control-plane eligibility signal. It means AGN has enough
site and gateway identity information to consider the site for overlay
generation. It does not prove that Praxis has loaded the latest routing config
or authorized a particular request.

```yaml
spec:
  egress:
    address: provider.example.com:8443
    tls:
      mode: Mutual
      serverName: provider.example.com
  trust:
    canonicalFingerprints:
      - "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
```

Pins are lowercase SHA-256 digests of the leaf certificate's canonical DER
bytes, with no separators. Verify them through an independent trust channel
before configuration. The demo's tooling derives the canonical value from the
staged certificate; production certificate tooling should expose the same
DER-based digest.

**Certificate rotation:** `canonicalFingerprints` accepts one current pin and
one next pin. Add the next pin before deploying the new gateway certificate,
wait for the live and SWIM-advertised certificate state to converge, then remove
the old pin. An unexpected third identity is rejected. Trust failures demote an
Active site to `Connecting`, while connection failures demote it to
`Unreachable`; both phases exclude its CRDT providers from routing.

Private keys are never broadcast.  The operator reads only the `tls.crt` key from
the site certificate Secret — the `tls.key` key is never accessed for broadcast
purposes. The local operator reads its own `tls.key` only to authenticate the
bounded mTLS health probe. The provider gateway separately enforces peer identity
on every request.

**Routing eligibility:** Remote CRDT provider records are included in the routing overlay
only when the source `GridSite.status.phase == Active`.  Records from peers in any other
phase (`Discovered`, `Connecting`, `Unreachable`, or missing) are excluded at the
control-plane overlay level.

Active phase indicates the control plane completed the configured gateway probe.
Request readiness still requires routing configuration propagation and
provider-side authorization, which are enforced separately by the data plane.

## Separation of Concerns

| Who | What |
|-----|------|
| **AGN Operator** | Validates provider credential `secretRef`; projects credential references (never token values) into routing overlays; can render opt-in consumer Praxis `ConfigMap`; generates local CA and site cert Secrets; marks `GridSite.status.phase = Active` after the configured identity-aware gateway probe succeeds. |
| **Gateway filters** | `intelligent_route` selects candidates and writes credential metadata; `credential_inject` reads a mounted Secret file and injects credentials per request; `peer_identity_trust` verifies peer certificate identity on provider gateways. |
| **Deployment / platform** | Provisions gateway trust material (CA cert or cert bundle) at the path referenced by the consumer config's `ca_path`; distributes the Grid CA cert to remote clusters where gateways need to verify peer identity; configures the provider gateway's peer identity filter; manages gateway rollout when trust material changes. |
| **Workload** | Sends requests to the Gateway, optionally with routing headers — never handles provider credentials. |

`Active` GridSite status is the control-plane eligibility gate: it controls whether a remote
site's providers appear in the routing overlay. Active means the control plane has enough
trust information to include the site in routing decisions.

Secure data-plane traffic readiness requires additional steps beyond Active status: gateway
trust material provisioning (CA cert or cert bundle), peer identity filter configuration,
routing configuration loading, and provider authorization. These are deployment prerequisites
and runtime readiness checks, not automatic outputs of AGN's gateway health evaluation.
