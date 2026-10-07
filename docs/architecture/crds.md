# Custom Resource Definitions

API group: `grid.praxis.fast/v1alpha1`

The AI Grid Network (AGN) Operator defines these resources to describe sites,
provider capacity, and routing policy. The established API identities remain
unchanged by the project-name update.

All CRDs are cluster-scoped.

## GridNetwork

The AGN logical network and top-level tenancy scope. A single
cluster can host multiple `GridNetworks` for
multi-tenancy.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
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
        clusterEndpoints:           # endpoint topology for load_balancer
          - cluster: site-a
            address: "10.0.0.4:30080"
            transport:
              mode: mutual_tls         # mTLS with CA verification and client cert
              sni: site-a.grid.internal
          - cluster: api-provider
            address: "mock-api.default.svc:8080"
            transport:
              mode: plaintext          # explicit insecure/dev-only — no TLS
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
  budgetPolicy:                   # optional; absent means no tenants are tracked
    tenants:
      - tenantId: tenant-a
        capUsd: 100.0
      - tenantId: tenant-b
        capUsd: 250.0
```

### Routing policy fields

These optional `GridNetwork.spec` fields control provider ordering, admission,
and request selection. For guidance on composing them to achieve a routing
behavior, see the [AGN Routing Guide](../routing.md).

| Field | Supported values / shape | Default and interaction |
|---|---|---|
| `routingPolicy` | `geographyFirst`, `scoreFirst` | `geographyFirst`. Controls candidate ordering and selection-group boundaries. |
| `scoringPolicy.strategy` | `noMetrics`, `queueDepth`, `kvCachePressure` | `noMetrics`; when `scoringPolicy` is present, `strategy` is required. |
| `selectionPolicy.mode` | `deterministic`, `roundRobin`, `random`, `weightedRandom` | Omitted from the overlay when unset; Praxis then uses deterministic selection. |
| `placementPolicy.strategy` | `static` | Required if and only if selection mode is `weightedRandom`; rejected with other modes. |
| `admissionPolicy.mode` | `instantaneous`, `stabilized` | `instantaneous`; stabilized behavior requires repeated pressure/recovery observations. |
| `admissionPolicy.missingMetrics` | `existingOnly`, `excluded` | `existingOnly` when admission policy is configured. Applies when an active scoring strategy and provider signal are configured but the signal is missing or expired; it does not activate metrics observation. |
| `admissionPolicy.pressure` | Six fields: `enterThreshold`, `exitThreshold`, `failureThreshold`, `successThreshold`, `minimumStateDuration`, `recoveryHoldDown` | When `pressure` is omitted, defaults are `0.85`, `0.70`, `2`, `3`, `10s`, and `30s`, respectively. When supplied, all six fields are required. Exit must be lower than enter; durations must be positive whole seconds (for example, `10s`). |
| `metricsRefreshInterval` | Seconds or milliseconds, at least one second | `300s`; TLS-protected metrics cap the effective interval at `60s`. Controls metric refresh/re-ranking, not request-time selection. |

`geographyFirst` and `scoreFirst` are the only current routing-policy values.
Explicit grouping fields such as `selectionPolicy.grouping.localityScope` are
not part of this CRD.

**Phases**: Pending → Initializing → Active → Degraded

**Status fields**: `gridId`, `connectedSites`, `distributedProviderCount`,
`observedGeneration`, `phase`, `consumerConfigStatus[]`, `budgetStatus[]`

`distributedProviderCount` reflects the number of remote `InferenceProvider`
records received from peer sites via CRDT broadcast.  Local providers and records
from other `GridNetwork`s are excluded from the count.

`consumerConfigStatus[]` is populated for each gateway with
`consumerConfig.enabled: true`, reporting the outcome of the most recent
render/apply attempt.

### Tenant budget tracking

`budgetPolicy.tenants[]` opts individual tenants into cumulative spend
tracking. AGN merges each site's locally recorded spend for a tenant into a
per-tenant CRDT counter (a `GCounter`, one slot per originating site) that is
gossiped over SWIM alongside provider state, so the reported total reflects
spend recorded anywhere in the grid, not just the local site.

For every tenant declared in `budgetPolicy`, `budgetStatus[]` reports:

- `tenantId` — matches `budgetPolicy.tenants[].tenantId`
- `capUsd` — copied from the policy, in USD
- `spendUsd` — the converged cross-site total, in USD
- `spendRatio` — `spendUsd / capUsd`, for at-a-glance dashboarding

`budgetStatus[]` is a status **signal only**. AGN does not itself degrade or
reject traffic when a tenant's `spendRatio` reaches or exceeds `1.0` — that
enforcement decision is expected to live in a gateway-side policy filter
(cross-repo, `praxis-ai`), the same split used for `provider_route`
authorization. Real per-request tenant attribution also depends on
upstream work (`praxis-ai#130`/`praxis-ai#104`) and does not exist yet.

Because `budgetStatus[]` is visible to any caller with read access to the
`GridNetwork` resource, and Kubernetes RBAC is not field-level, a reader
authorized to view one tenant's status can see every other tracked tenant's
spend on the same `GridNetwork`. See
[`grid#48`](https://github.com/praxis-proxy/grid/issues/48) for the
options under consideration if per-tenant confidentiality is required.

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

`spec.seeds` is a list of SWIM endpoints (`host:port`) used to bootstrap
SWIM mesh formation. Each entry may be a literal IPv4 address, a bracketed IPv6
address, or a DNS hostname. Hostnames are resolved with a bounded lookup before
they are announced to the running SWIM runtime on every
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

**`spec.tls.swimKeyRef`:** References a Kubernetes Secret containing the 32-byte
AES-256-GCM key for SWIM transport authentication.  When configured, the
`GridNetwork` controller reads the key from the Secret and configures the
SWIM runtime to encrypt all outgoing UDP packets and reject incoming packets
that fail authentication before it announces CRD seeds or publishes
certificate/provider state for that reconcile.

The Secret must contain a `"key"` field (or the field named by `swimKeyRef.key`)
with exactly 32 bytes.  If the Secret is absent, unreadable, or has the wrong
length, the reconcile fails before CRD seed announcement and state broadcast.
The process-global SWIM runtime keeps any previously loaded key until restart;
it does not switch to plaintext for that configured reconcile.

For local development and testing, the `GRID_SWIM_ENCRYPT_KEY` environment
variable (64-character hex) provides an alternative key injection path without
requiring a Kubernetes Secret.  Because environment variables are visible to
same-host process inspectors, this is not the production Secret delivery path.

Routing eligibility remains gated separately by `GridSite.status.phase == Active`
regardless of SWIM encryption configuration. Active status indicates control-plane
eligibility only: sufficient trust information exists to include the site in routing
overlays. Data-plane security readiness (mTLS handshake completion, client certificate
validation, routing config loading) is verified separately at request time.

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

With site auto discovery on, the same TTL bounds auto-discovered GridSites. When
gossip stops vouching for a stub's site (any SWIM state but `Dead`), the operator
records `status.absentSince` and clears it if the site returns. Once that is at
least `N` seconds old, the stub is deleted and no longer counts against the
256-site discovery cap. The delete is conditional on the object being unchanged,
so a site that rejoins first keeps its stub. Declared GridSites are never deleted.
With the TTL absent, stubs use a 24-hour default, so the cap always drains; the
overlay still keeps stale candidates.

The operator judges no site absent until it has been up for one gossip
verification window (a 10-minute record expiry plus convergence), so a restart
never starts clocks or collects. A pass that would delete more than half the
stubs, and more than 8, deletes none and logs a warning; a partition looks like
mass departure.

### GatewayRef.consumerConfig

`spec.gatewayRefs[].consumerConfig` opts a gateway into operator-managed consumer
Praxis `ConfigMap` generation.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | `false` | Set to `true` to enable consumer config generation for this gateway. |
| `credentialMountBase` | `/run/secrets/grid-credentials` | Base directory where credential Secrets are mounted inside the consumer pod. |
| `configMapName` | `praxis-consumer-config` | Name of the generated `ConfigMap` in the gateway namespace. |
| `clusterEndpoints[]` | `[]` | Endpoint topology for `load_balancer` clusters. Each entry maps a candidate cluster name to an address with explicit `transport` configuration. Missing transport fails closed. |
| `clusterEndpoints[].transport.mode` | _(required)_ | `mutual_tls` (mTLS with CA/client cert/SNI/verify) or `plaintext` (no TLS, insecure/dev-only). |
| `clusterEndpoints[].transport.sni` | _(required for `mutual_tls`)_ | TLS Server Name Indication; must match the provider certificate SAN. |
| `tlsCertMountPath` | `/etc/praxis/tls` | Base path for mounted TLS files used when a `clusterEndpoints[]` entry uses `mutual_tls` transport. |
| `listenerPort` | `8080` | HTTP port for the generated `listeners[0].address` (`0.0.0.0:{listenerPort}`). |

When `enabled: true`, the `GridNetwork` controller renders a `praxis.yaml`-keyed
`ConfigMap` in the gateway namespace on each reconcile.  The generated config is a
complete, runnable Praxis config containing:

- `listeners:` — one public listener at `0.0.0.0:{listenerPort}`
- `filter_chains:` — the consumer chain with:
  - `intelligent_route` candidates from the routing overlay (with `credential.secretRef` for
    credential-bearing candidates)
  - `credential_inject` entries (one per unique credential reference) using
    `file:` sources — token bytes are never written to the `ConfigMap`
  - `load_balancer` entries (one per unique candidate cluster). Every referenced
    cluster must have a matching `clusterEndpoints[]` entry with endpoint address
    and explicit `transport` configuration.  `transport.mode` is the security
    switch — not `sni` presence.  Missing transport fails closed
- `admin:` — admin listener at `127.0.0.1:9901`
- `shutdown_timeout_secs: 5`

The generated credential-injection config assumes the gateway is the egress
component for the selected backend.  This is correct for direct API-provider and
cloud-provider fallback routes.  For remote provider sites, provider credentials
should be mounted only in the remote site or provider-side component that makes
the final backend call.

The `credential_inject` filter is a Praxis AI runtime dependency. The AGN
operator can render the config shape, but the deployed Praxis AI image must
include that filter for the generated config to start successfully.

When `enabled: false` or `consumerConfig` is absent, this gateway behaves as before
— only the routing overlay `ConfigMap` is applied.

## GridSite

Represents another site in the grid. Created manually
for seed peers or automatically by SWIM discovery.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridSite
metadata:
  name: cluster-b
  labels:
    grid.praxis.fast/network: production
spec:
  gridNetworkRef: production
  egress:
    address: egress.cluster-b.example.com:8443
    tls:
      mode: Mutual
      serverName: egress.cluster-b.example.com
  trust:
    canonicalFingerprints:
      - "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  region: us-east-1
  zone: us-east-1a
  sovereigntyZone: us
```

**Phases**: Pending → Discovered → Connecting → Active → Unreachable → Left

**Status fields**: `phase`, `reason`, `message`, `observedGeneration`,
`publicCertPem`, `capabilities` (inference, agentTools, agentToAgent),
`lastProbeTime`, `lastTransitionTime`, `absentSince` (auto-discovered sites only)

### GridSite lifecycle

SWIM discovery, authentication, and authorization are separate concerns:

- SWIM discovery identifies a peer and records liveness.
- Authentication proves the peer gateway identity, normally through mTLS
  certificate validation.
- Authorization decides whether that authenticated peer is allowed to
  participate in the AI Grid Network or carry traffic for a given policy scope.

A discovered SWIM peer is not automatically authorized for routing.

| Phase | How entered | Transition driver |
|---|---|---|
| `Pending` | Resource created (manually or by auto-discovery) | Initial default |
| `Discovered` | SWIM peer observed as Alive | `GridNetwork` controller writes on first observation |
| `Connecting` | Gateway address known (`spec.egress.address` non-empty) | `GridSite` controller advances from Discovered; performs identity-aware probe |
| `Active` | `TlsVerified` | `GridSite` controller promotes from Connecting only after identity-verified TLS succeeds |
| `Unreachable` | Connectivity failure while Active | `GridSite` controller moves Active → Unreachable when the endpoint cannot be reached |
| `Left` | Set on graceful site departure | Preserved by operator once set |

**Reason codes** (in `status.reason`):

| Reason | Phase | Meaning |
|---|---|---|
| `AwaitingDiscovery` | Pending | Site record exists; SWIM has not yet observed the peer as Alive |
| `SWIMDiscovered` | Discovered | Peer observed as Alive in SWIM membership; gateway address propagating |
| `GatewayAddressKnown` | Connecting | Gateway address received; advancing to Connecting |
| `GatewayAddressMissing` | Discovered | No gateway address known; see `GRID_GATEWAY_ADDRESS` |
| `EgressMissing` | Connecting or Unreachable | A previously probed site has no egress address |
| `TlsVerified` | Active | TLS handshake succeeded; certificate chain, identity, and configured pin verified |
| `IdentityVerificationRequired` | Connecting | TCP endpoint is reachable, but plaintext cannot establish the gateway identity |
| `PlaintextUnreachable` | Connecting or Unreachable | TCP probe failed (explicit Plaintext mode) |
| `ConnectTimeout` / `ConnectionFailed` | Connecting or Unreachable | TCP connection timed out or failed |
| `HandshakeTimeout` / `TlsProtocolError` | Connecting | TLS handshake timed out or failed |
| `UntrustedIssuer` | Connecting | Server certificate does not chain to the configured AGN trust root |
| `IdentityMismatch` | Connecting | Server SAN does not match configured `serverName` |
| `CertificateExpired` / `CertificateNotYetValid` | Connecting | Server certificate is outside its validity period |
| `PinMismatch` | Connecting | Canonical fingerprint does not match a configured pin |
| `AdvertisedCertMismatch` | Active | SWIM-advertised certificate does not match a configured pin; recorded only, since the live leaf verified |
| `TrustMaterialMissing` | Connecting | CA, client certificate, key, server name, or pin policy is absent |
| `TrustMaterialInvalid` | Connecting | Trust material is malformed, oversized, or uses the deprecated fingerprint format |

**GridSite phase transitions:**

- Pending → Discovered: the `GridNetwork` controller writes `Discovered` when a remote SWIM
  peer is first observed as Alive (requires `grid.praxis.fast/auto-discover-sites: "true"`
  label on the `GridNetwork`).
- Discovered → Connecting: the `GridSite` controller advances automatically when
  `spec.egress.address` is non-empty. For auto-discovered sites, the egress address comes from
  the remote operator's `GRID_GATEWAY_ADDRESS` env var, propagated via SWIM state broadcast.
  If the remote operator has not configured `GRID_GATEWAY_ADDRESS`, the egress address is empty
  and the site stays Discovered with reason `GatewayAddressMissing`.
- Connecting: the `GridSite` controller probes the egress gateway on each reconcile.  For
  `Mutual` TLS mode, the probe performs a bounded TLS handshake verifying the CA chain,
  `serverName` SAN, and required `canonicalFingerprints` pin. For explicit
  `Plaintext` mode, it performs a bounded TCP connect for diagnostics but keeps
  the site in `Connecting`; TCP reachability alone cannot make a site
  routing-eligible. Active also requires mTLS client credentials
  (`siteSecretRef` must be configured on the `GridNetwork`).
- Active → Unreachable: the `GridSite` controller demotes Active to Unreachable when the probe
  cannot connect. Identity or trust failures demote Active to Connecting, distinguishing a
  reachable but unverified endpoint from an unreachable endpoint.

**Metrics:** `grid_site_phase{site,phase}` reports each `GridSite`'s phase as a state
set, the shape of kube-state-metrics' `kube_pod_status_phase`. The series for the
current phase is 1 and the other five are 0, so a site is ready when
`grid_site_phase{phase="Active"} == 1`. It follows the phase the printer column shows,
updates on every `GridNetwork` reconcile, and drops a site's series once its `GridSite`
is gone. Cardinality is six series per site. `grid_site_phase_transition_total`
still counts the transitions by phase and reason.

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

**`spec.egress.tls` fields:**

| Field | Meaning |
|---|---|
| `mode` | `Mutual` (default) — TLS handshake with CA verification and client auth; `Plaintext` — TCP-only diagnostics that never become routing-eligible |
| `serverName` | Expected DNS identity for TLS SNI and SAN verification; required for `Mutual`, must be absent for `Plaintext` |

**`spec.trust` fields:**

| Field | Meaning |
|---|---|
| `canonicalFingerprints` | Required DER-certificate SHA-256 pins (`hex(sha256(der_bytes))`), one or two entries for bounded rotation overlap |
| `certFingerprint` | **Deprecated.** Legacy PEM-based fingerprint; rejected at runtime with `TrustMaterialInvalid`; migrate to `canonicalFingerprints` |

**Routing eligibility:** `GridSite.status.phase == Active` is the control-plane eligibility
gate for remote CRDT provider records. Active means the control plane has
verified the remote site's identity through TLS, which is sufficient to include
the site's providers in routing overlays for consideration.

Provider records from a peer whose `GridSite` is in `Discovered`, `Connecting`, `Pending`,
`Unreachable`, or `Left` are excluded from the routing overlay. Records from a peer with
no matching `GridSite` are also excluded (fail-closed).

GridSite Active is a control-plane eligibility signal. It means AGN has enough
site/trust information to consider the site for overlay generation. It does not
prove that a Praxis gateway has loaded the latest routing config or authorized
provider-side traffic. Data-plane readiness is verified separately at request
time by provider gateway filters.

**Single-site / combined-site deployments:** Site discovery is driven by *remote*
SWIM peers - the `Pending -> Discovered` transition above requires a remote peer
observed Alive. A single or combined cluster has no peers, so its own `GridSite`
stays `Pending` with reason `AwaitingDiscovery`. **This is expected and does not
block local serving:** local `InferenceProvider`s are eligible regardless of
`GridSite.status.phase`; only *remote* CRDT provider records are phase-gated (see
"Routing eligibility" above). Do not add SWIM seeds or extra operator replicas to
try to force the site `Active` - there is no second site to discover, and a lone
operator legitimately runs a single-node mesh with zero peers. The `Active` phase
and its mTLS gateway probe (`spec.egress` + `spec.trust`) apply to reaching
*remote* sites, or a manually-configured peer gateway endpoint. See
[Architecture Overview -> Single-Site and Combined Deployments](overview.md#single-site-and-combined-deployments).

See [Routing eligibility](routing.md#routing-eligibility) for the full gating rule.

Example status — Mutual TLS verified:

```yaml
status:
  phase: Active
  reason: TlsVerified
  message: "TLS handshake succeeded; certificate chain, identity, and pin verified"
  observedGeneration: 5
  lastProbeTime: "2026-07-30T12:00:00Z"
  lastTransitionTime: "2026-07-30T11:55:00Z"
```

Example status — trust material missing:

```yaml
status:
  phase: Connecting
  reason: TrustMaterialMissing
  message: "required trust material not available"
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

### Status conditions (acceptance criterion open)

The current `GridSite` status uses a flat
`phase`/`reason`/`message` model rather than a
Kubernetes-style `conditions[]` array.

Issue #11's acceptance criteria include a conditions-based
status contract.  That criterion is **not yet resolved**:
it requires either (a) implementing a `conditions[]` array
or (b) formally amending the issue to accept the
`phase`/`reason`/`message` model as the replacement.
Neither has happened at the time of writing.

The flat model is the current contract:

- `phase` provides the primary lifecycle state;
  `reason` encodes the machine-readable probe outcome;
  `message` gives a bounded human-readable explanation.
- Richer conditions (e.g. `Reachable`, `IdentityVerified`,
  `CertificatePinned`) may be added when multi-signal
  readiness is needed (see `docs/architecture/overview.md`,
  Trust and Readiness section).

Tools and automation **must not** depend on a
`conditions[]` field existing.  Match on `phase` for
coarse state and on `reason` for specific probe outcomes.

## InferenceProvider

Represents an inference backend available over the
grid.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: InferenceProvider
metadata:
  name: openai-api
spec:
  gridNetworkRef: production
  providerKind: open_ai          # open_ai | anthropic | bedrock | vertex | self_hosted
  backendKind: api_provider     # local | remote | cloud_managed | api_provider
  endpoint: https://api.openai.com
  models:
    - name: gpt-5-mini
      capabilities: [text_generation]
  auth:
    strategy: bearer_token      # current native path; see Auth doc
    secretRef:
      name: openai-token
      namespace: praxis-system
      key: token
  accessPolicy:
    siteSelector:
      matchLabels: {}           # empty = all sites
  siteSelector:
    matchLabels: {}
```

This external API example intentionally omits `healthCheck`: AGN probes health
with an unauthenticated HTTP `GET`, which is not the provider's authenticated
inference API. It also omits metrics scraping because the external API does not
provide the provider-pool metrics used by AGN scoring. The referenced
`openai-token` Secret must exist in `praxis-system` before controller-managed
credential projection can become available.

**Phases**: Pending → Available → Degraded → Unavailable

**Readiness**: `status.conditions` carries a `Ready` condition, written by the
operator's signals loop from each scrape of the provider's metrics. Readiness is
a condition, not a phase: `phase` follows the provider's configuration only, and
says nothing about whether it serves now.

| Status | Reason | When |
|---|---|---|
| `True` | `Ready` | The latest scrape succeeded with at least one ready endpoint. |
| `False` | `NoEndpointsReady` | Two consecutive scrapes counted zero ready endpoints, and the EPP recorded no engine answer in the last 30s. |
| `Unknown` | `NoLivenessCheck` | The scrape answered without the pool's ready-endpoint series (`llm_d_epp_ready_endpoints`, then `inference_pool_ready_pods`, for `poolName`). Readiness is unknown rather than false, so the provider is not excluded: a provider pointed at vLLM's own `/metrics` carries no such series. The message names what was missing. |
| `False` | `ScrapeTimedOut` | No scrape succeeded within `staleMetricsSeconds`, and the latest timed out. |
| `False` | `ScrapeUnauthorized` | As above, and the latest was refused with 401 or 403. |
| `False` | `TLSHandshakeFailed` | As above, and the latest failed TLS, including the TLS material. |
| `False` | `ScrapeFailed` | As above, and the latest failed otherwise. The message names the class: `dns`, `connect`, `http`, `body_cap`, `parse`, or `config`. |
| `False` | `MetricsStale` | No scrape succeeded within `staleMetricsSeconds`, and none failed. |
| `False` | `ProviderUnavailable` | The provider is `Unavailable`. |
| `Unknown` | `AwaitingFirstScrape` | No scrape has succeeded yet, within the grace window. |
| `Unknown` | `MetricsNotConfigured` | No `metricsConfig`, so readiness cannot be read. |

The operator logs each change of reason once: at WARN when the provider turns not
ready, at INFO when it returns to `Ready` or waits. Each scrape counts in
`grid_provider_scrape_total{grid_provider,result}`, where `result` is `success`,
`no_series`, or a failure class, and
`grid_provider_last_scrape_success_timestamp_seconds{grid_provider}` holds the time
of the last scrape with the ready-endpoint series.

A provider whose `Ready` is `False` is excluded from routing: its site publishes
`grid_provider_ready 0`, and its serving config entry carries `admission: none`.
See [Polling Cross-Site Load Signals](polling-metrics.md#provider-readiness).

The READY column reads the condition's status directly, so no status field
repeats it. `-o wide` adds REASON, the condition's reason, and PHASE.

```text
NAME           PROVIDER      READY   AGE
qwen3-site-a   self_hosted   True    3d
qwen3-site-b   self_hosted   False   3d
```

`spec.capacityWeight` is an optional positive relative provider capacity from
`1` through `1000`, used only with `GridNetwork.spec.selectionPolicy.mode:
weightedRandom` and `placementPolicy.strategy: static`. If omitted, the
effective weight is `1`. AGN copies this value to the overlay's
`traffic_weight`; it does not represent a percentage.

### Backend kind

`spec.backendKind` describes the provider's placement and policy category:

| Value | Meaning |
|-------|---------|
| `local` | Self-hosted capacity in the local site. |
| `remote` | Self-hosted capacity in another AGN site. |
| `cloud_managed` | Managed cloud capacity controlled by the operator's cloud account. |
| `api_provider` | External API/SaaS provider used as fallback or explicit API route. |

The value influences scoring and routing policy. It does not require a specific
transport implementation; for example, a `cloud_managed` backend can still be
fronted by Praxis.

The current CRD schema represents `backendKind` as a string rather than an
enum. The four values above are the categories AGN recognizes for routing and
scoring conversion; an unrecognized value is not converted into a normal
scoring candidate. Use a listed value unless the implementation is extended.

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

3. The consumer Praxis config uses `credential_inject` with a `file:` source
   pointing at a mounted Secret file.

Token bytes do not appear in the overlay `ConfigMap`, `intelligent_route` candidates,
filter metadata, or the consumer Praxis `ConfigMap`.

### Metrics configuration

`spec.metricsConfig` configures the operator-side Prometheus scrape used during
`GridNetwork` reconciliation. When present, the operator scrapes
`{spec.endpoint}{metricsConfig.path}`, parses the configured signal names, and
feeds the resulting `BackendMetrics` into overlay scoring.

| Field | Default | Meaning |
|-------|---------|---------|
| `metricsEndpoint` | absent | Optional metrics-service base URL. When set, it replaces `spec.endpoint` as the scrape base; `path` is appended to the selected base. |
| `path` | `/metrics` | HTTP path, relative to `metricsEndpoint` when set, otherwise `spec.endpoint`. |
| `timeout` | `2s` | Scrape timeout. `s` and `ms` suffixes are recognized. |
| `poolName` | absent | Selects samples whose Prometheus `name` label matches this pool. When set, the scrape must contain at least one configured signal for that pool. |
| `queueCapacity` | absent | For raw queue-depth counts, divide by this positive capacity and clamp the normalized value to `0.0..1.0`. Without it, queue depth must already be normalized. |
| `signalNames` | all unset | Mapping from scoring signals to Prometheus metric names. |
| `staleMetricsSeconds` | absent | Maximum age in seconds for reusing the last successful sample after a failed scrape. Minimum: `1`. For plaintext metrics, absence means immediate neutral fallback; when TLS is configured, an expired/absent sample makes the provider unhealthy and excluded. |
| `tls` | absent | Optional metrics-specific TLS override. Otherwise `spec.tls` applies. See [TLS and mTLS](#tls-and-mtls). |

Providers without `metricsConfig` and signals without configured metric names
use neutral metric scores. For scrape failures, plaintext configuration retains
the neutral-scoring compatibility behavior; TLS-configured metrics fail closed
after any configured stale-sample grace period expires. See
[Stale metrics grace period](routing.md#stale-metrics-grace-period) in the
routing architecture for full semantics.

#### Signal names

| Field | Expected value |
|-------|----------------|
| `queueDepth` | Normalized queue depth from `0.0` to `1.0`. |
| `kvCacheUtilization` | KV-cache utilization from `0.0` to `1.0`. |
| `latencyP99Ms` | P99 request latency in milliseconds. |
| `prefixCacheHitRatio` | Prefix-cache hit ratio from `0.0` to `1.0`. |
| `errorRate` | Error rate from `0.0` to `1.0`. |
| `healthy` | Health gauge interpreted by the metrics parser. |
| `readyEndpoints` | Ready endpoints in the pool, read for the `Ready` condition. Defaults to `llm_d_epp_ready_endpoints`, then `inference_pool_ready_pods`, filtered by `poolName`. |

#### TLS and mTLS

`spec.tls` configures TLS for provider health checks, metrics scraping, and
model discovery. The operator resolves the same CA bundle and optional client
identity for each request. This keeps server verification and mTLS consistent
across the provider's control-plane checks.

The `tls` fields under `healthCheck`, `metricsConfig`, and
`modelDiscovery.openAiModels` remain available as per-feature overrides for
endpoints that need different trust material or a different client identity.
Without an override, each feature inherits `spec.tls`; if neither is set, the
request uses system root certificates. Configured custom CA material replaces
system roots for that request, and invalid TLS material fails closed.

| Field | Required | Meaning |
|-------|----------|---------|
| `spec.tls.caSecretRef` | one of | Secret containing the CA certificate for server verification. Default key: `ca.crt`. |
| `spec.tls.caConfigMapRef` | one of | ConfigMap containing the CA certificate, such as the platform service CA (`openshift-service-ca.crt`, key `service-ca.crt`). Default key: `ca.crt`. |
| `spec.tls.clientCertificateSecretRef` | no | Secret containing client certificate and private key for mTLS. |
| `spec.tls.clientCertificateSecretRef.certificateKey` | no | Key within `Secret.data` for the certificate PEM. Default: `tls.crt`. |
| `spec.tls.clientCertificateSecretRef.privateKeyKey` | no | Key within `Secret.data` for the private key PEM. Default: `tls.key`. |

Set exactly one of `caSecretRef` and `caConfigMapRef`; the configured CA is the only
trust used. `caSecretRef` follows the same [`SecretRef`](#credential-projection) schema used by
`spec.auth.secretRef`. `caConfigMapRef` takes `name`, `namespace`, and an optional `key`.  `clientCertificateSecretRef` adds explicit
`certificateKey` and `privateKeyKey` fields with serde defaults.

**Failure behavior**: when TLS material cannot be resolved or parsed, the
provider phase is downgraded from `Available` to `Degraded` with a
machine-readable `status.reason`:

| Reason | Category | Meaning |
|--------|----------|---------|
| `MetricsTlsSecretMissing` | reconcile-time | A referenced Secret does not exist. |
| `MetricsTlsKeyMissing` | reconcile-time | The expected key is absent from `Secret.data`. |
| `MetricsTlsMaterialInvalid` | reconcile-time | PEM material could not be parsed. |
| `MetricsTlsIdentityMismatch` | reconcile-time | Client certificate and private key do not match. |

Scrape-time handshake, authorization, timeout, and transport failures are
classified in operator logs, not surfaced as `InferenceProvider.status.reason`.
The last successful sample is reused only within `staleMetricsSeconds`; after
that, a TLS-configured provider is marked unhealthy (`healthy: false`) and
excluded from routing. Without TLS, scrape failures retain the neutral-scoring
compatibility behavior.

There is no `insecureSkipVerify` option.

Example (one-way TLS shared across provider checks):

```yaml
spec:
  tls:
    caSecretRef:
      name: provider-ca
      namespace: grid-system
  metricsConfig:
    path: /metrics
    timeout: 2s
    signalNames:
      queueDepth: vllm:num_requests_waiting
```

Example (mTLS):

```yaml
spec:
  tls:
    caSecretRef:
      name: provider-ca
      namespace: grid-system
    clientCertificateSecretRef:
      name: provider-client-cert
      namespace: grid-system
      certificateKey: tls.crt
      privateKeyKey: tls.key
  metricsConfig:
    path: /metrics
    timeout: 2s
    signalNames:
      queueDepth: vllm:num_requests_waiting
```

#### Bearer authentication

An llm-d EPP serves `/metrics` behind TokenReview and SubjectAccessReview by
default. `metricsConfig.auth` sends a credential the EPP can authorize.

| Field | Meaning |
|-------|---------|
| `auth.type` | `serviceAccountToken`: a short-lived token for the grid metrics scraper ServiceAccount, sent as `Authorization: Bearer`. |
| `auth.allowPlaintext` | Send the credential over `http://`. Default `false`. For a lab only. |

Whoever runs the EPP receives the token, and it is valid against the API server, so
the operator never sends its own token. The grid-operator chart creates a scraper
ServiceAccount allowed only `get` on the nonResourceURL `/metrics`
(`rbac.metricsScraper`, default `true`), and the operator mints a 10-minute token for it
with the TokenRequest API, reusing it until two thirds of its lifetime has passed (about
400 seconds). The token is not bound to a Pod: the API server binds a token only to a Pod
running as the token's own ServiceAccount, and the operator runs as another. The token is
never logged. A failed mint logs at WARN when the error changes.

A credential goes only to a host proven by the CA in `spec.tls` or the
`metricsConfig.tls` override: with `auth` set, an `https://` endpoint without
either TLS setting is refused rather than trusted through the system roots,
and an `http://` endpoint is refused unless `allowPlaintext` is set.

Example (an EPP serving its metrics with the platform service CA):

```yaml
spec:
  tls:
    caConfigMapRef:
      name: openshift-service-ca.crt
      namespace: grid
      key: service-ca.crt
  metricsConfig:
    metricsEndpoint: https://qwen3-epp-service.ai-tenant-site-a.svc:9090
    path: /metrics
    poolName: qwen3-inference-pool
    signalNames:
      queueDepth: llm_d_epp_average_queue_size
      kvCacheUtilization: llm_d_epp_average_kv_cache_utilization
    auth:
      type: serviceAccountToken
```

#### Queue depth normalization

AGN does not normalize raw queue counts. Exporters should publish
`queueDepth` as a normalized `0.0`–`1.0` gauge before the operator scrapes it.

### Model discovery

`spec.modelDiscovery` makes the operator poll the backend for the models it
serves. Discovery does not yet affect routing or gossip; `spec.models`
remains the routing source.

```yaml
spec:
  endpoint: https://vllm:8000
  tls:                          # shared by health, metrics, and discovery
    caSecretRef:
      name: provider-ca
      namespace: grid-system
  metricsConfig:
    path: /metrics
  healthCheck:
    path: /health
  modelDiscovery:
    openAiModels:               # GET {endpoint}{path}, reads data[].id
      endpoint: https://vllm:8000 # optional; defaults to spec.endpoint
      path: /v1/models          # default
```

Use `healthCheck.tls`, `metricsConfig.tls`, or
`modelDiscovery.openAiModels.tls` only when that endpoint needs a different
CA bundle or client certificate from the shared `spec.tls` settings.

The effective request URL appears in `status.modelDiscoveryUrl`. It reflects
the configured endpoint and path, regardless of whether a poll succeeds, and
is absent when model discovery is not configured.

A failed poll sets `status.modelDiscoveryError` to the bounded failure category
used by `grid_model_discovery_total`. A successful poll clears it. The discovery
loop owns this field separately from the provider's routing phase and clears
it when discovery is disabled. The field reports the latest poll error; the
held model set still follows its TTL independently.

The bearer token comes from `spec.auth`. A model-discovery URL must use HTTPS
when a bearer token is configured. With `auth.manual`, requests carry no
credentials, so plain HTTP remains available.

Discovery runs in its own loop, like the signals scraper, not in reconcile.
Discovered sets are held in memory per provider:

- A successful poll replaces the set and renews its deadline; an empty list is
  a valid result.
- A failed poll keeps the set; its deadline is not renewed.
- A set not renewed within the TTL expires. Absence is the staleness signal,
  so a stale set fails closed and no clocks are compared.
- A provider that is deleted or drops `modelDiscovery` loses its set on the
  next round.
- The response is rejected as a whole, never partially applied, when it:
  - is not a 2xx, or exceeds 1 MiB;
  - is not JSON with `data[].id`;
  - has a blank id, an id over 256 bytes, or an id with control characters;
  - repeats an id (duplicates are treated as malformed, not deduplicated);
  - lists more than 256 models.

| Variable | Default | Meaning |
|----------|---------|---------|
| `GRID_MODEL_DISCOVERY_INTERVAL_SECS` | `60` | time between rounds |
| `GRID_MODEL_DISCOVERY_TIMEOUT_SECS` | `5` | bound on one provider poll |
| `GRID_MODEL_DISCOVERY_TTL_SECS` | `180` | how long a set survives without a successful poll; at least interval + timeout |
| `GRID_MODEL_DISCOVERY_CONCURRENCY` | `8` | providers polled at once |

Each poll increments `grid_model_discovery_total{provider,outcome}`, where
`outcome` is one of `ok`, `credential`, `tls`, `config`, `unreachable`,
`timeout`, `http_status`, `invalid_response`. Failures are also logged.

## AgentToolProvider

Represents MCP tool servers available over the grid.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
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
        grid.praxis.fast/site: cluster-a
```

**Phases**: Pending → Available → Unavailable

`Degraded` is not currently reachable for this CRD: unlike
`InferenceProvider`'s metrics-scrape path, the MCP `tools/list` probe has no
partial-success state to represent — it either succeeds (`Available`) or
fails outright (`Unavailable`), mirroring `phase_and_reason_from_probe`'s and
`phase_from_matching`'s explicit design (both are tested to never emit
`Degraded`).

**Status fields**: `discoveredTools` (auto-populated
from MCP `tools/list`; a failed probe preserves the previous list rather than
clearing it), `matchingSites`, `reason` (machine-readable; see reason codes
below), `observedGeneration`

#### AgentToolProvider reason codes

| Reason | Phase | Meaning |
|---|---|---|
| `ProviderConfigInvalid` | Unavailable | Static spec validation failed (e.g. malformed `siteSelector`) before any `GridNetwork`/site/probe work runs. |
| `GridNetworkNotFound` | Unavailable | `spec.gridNetworkRef` does not resolve to an existing `GridNetwork`. |
| `CredentialSecretMissing` | Unavailable | `spec.auth.secretRef` does not resolve to an accessible Secret. |
| `McpEndpointUnreachable` | Unavailable | The MCP endpoint could not be reached: transport failure, DNS error, timeout, or a blocked (SSRF-sensitive) address. |
| `McpToolsListInvalidResponse` | Unavailable | The endpoint was reached but the `tools/list` exchange failed or returned an unparseable response. |
| `McpAuthRejected` | Unavailable | The MCP server rejected the configured `spec.auth` credentials (HTTP 401/403). |
| `McpAuthTokenInvalid` | Unavailable | The resolved `spec.auth` bearer token contains characters that cannot be sent as an HTTP header value; the probe fails closed rather than proceeding unauthenticated. |
| `EndpointTlsSecretMissing` | Unavailable | A referenced TLS Secret does not exist (or the requested key is absent — see [`grid#58`](https://github.com/praxis-proxy/grid/issues/58) for a known misclassification of the latter). |
| `EndpointTlsKeyMissing` | Unavailable | The expected key exists in the Secret but its value is empty. |
| `EndpointTlsMaterialInvalid` | Unavailable | CA certificate PEM material could not be parsed. |
| `EndpointTlsIdentityMismatch` | Unavailable | Client certificate or private key PEM material could not be parsed. |

An empty `reason` with `phase: Available` means `SitesMatched`; an empty
`reason` with `phase: Pending` means `AwaitingSiteMatch` — both are
telemetry-only labels (`grid_mcp_probe_total`, Events), not persisted to
`status.reason` itself.

## AgentToAgentProvider

Represents A2A agents available over the grid.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
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
        grid.praxis.fast/site: cluster-a
```

**Phases**: Pending → Available → Degraded → Unavailable
