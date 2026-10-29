# Custom Resource Definitions

API group: `grid.praxis.fast/v1alpha1`

The AI Grid Network (AGN) Operator defines these resources to describe sites,
provider capacity, and routing policy. The established API identities remain
unchanged by the project-name update.

All CRDs are cluster-scoped and served at `v1alpha1`.

## Status conditions

`GridNetwork`, `GridSite`, `InferenceProvider` and `SiteInvite` each carry
`Available`, `Progressing` and `Degraded` after their first reconcile, so one
client reads any of them the same way.

- `Available` is `True` with reason `Available` when every gating condition of
  the kind is `True`. Otherwise it copies the status, reason and message of the
  first gating condition, in the kind's order, that is not.
- `Degraded` is `True` when a non-gating condition is bad, and copies its reason
  and message. Otherwise it is `False` with reason `AsExpected`.
- `Progressing` is `True` with reason `NewGeneration` while a gating condition
  describes an older `metadata.generation`, or with a reason naming a change the
  operator expects to finish without a person. Otherwise it is `False` with
  reason `AsExpected`.
- A positive condition's `True` reason is its own type name. `Unknown` means only
  that the operator has not evaluated the condition: `AwaitingFirstCheck` or
  `AwaitingFirstScrape` before the first evaluation, or `NotEvaluated`.

Available `True` and Degraded `False` is healthy. Progressing `True` means wait.
Available `False` with Progressing `False` means act, on the reason given.

Every kind prints the same status columns: AVAILABLE, PROGRESSING and DEGRADED
read those conditions, and SINCE is when Available last changed. `-o wide` adds
MESSAGE, the Available message, then PHASE, kept while v1alpha1 has it, and on
`InferenceProvider` READY.

## SiteInvite

Declares an invitation on the signer cluster. The signer pins the site name and
jurisdictions, optionally binds the invite to a public key digest, and limits
its lifetime (default `24h`, from `5m` through `168h`). The spec is immutable;
delete and recreate an invite to change it.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: SiteInvite
metadata:
  name: east2
spec:
  siteName: east2
  jurisdictions:
    - eu-west
  lifetime: 24h
  allowDeletedName: false
  # publicKeySHA256: <64 lowercase hex characters>
```

The status reports the public token ID, the Secret reference holding the
one-time token, its expiry, redemption details, and conditions. Treat the
referenced Secret as a credential and deliver it and the grid CA bundle to the
joining site's operator namespace out of band. The token itself is never
published in status.

**Conditions**: `Minted`, `Redeemed` and `Issued` are the facts, and
`Available` says whether the token can still be presented. An invite is one-shot,
like a Job, so a spent invite reads `Available` `False` and stays not `Degraded`.

| Condition | Status | Reason | When |
|---|---|---|---|
| `Minted` | `True` | `Minted` | The signer recorded the token. |
| `Minted` | `False` | `Minting` | The token is checkpointed and the mint is in flight. `Progressing` is `True` with `Minting`. |
| `Minted` | `False` | `NameTaken`, `TokenOutstanding`, `NameDeleted` | The signer refused the mint. The message names the fix. |
| `Redeemed` | `True` | `Redeemed` | A site spent the token. |
| `Redeemed` | `False` | `AwaitingRequest`, `Expired`, `Revoked` | No site has spent it, its lifetime ended, or it was revoked. |
| `Issued` | `True` | `Issued` | The site's certificate was issued, today in the same step as redemption. |
| `Issued` | `False` | `AwaitingRequest`, `Expired`, `InviteRevoked` | No certificate was issued. |
| `Available` | `False` | `Redeemed`, `Expired`, `Revoked`, or the `Minted` reason | The token cannot be presented. |

`Ready` stays for v1alpha1 readers and follows `Available` under its earlier
reasons: `True` `Ready` while the token can be presented, `False` `Pending` until
it is minted, and `False` `Redeemed`, `Expired` or `Revoked` after.

In Kubernetes, the controller reads SiteInvite objects, stores each token in a
Secret, and revokes unused tokens when an admin deletes an invite.
Standalone enrollment does not start the controller. Enrollment enforces an
optional key binding. Jurisdiction assignment and the human approval gate remain
pending, so a redeemed token currently issues a certificate inline.

The chart stores token Secrets in its release namespace by default. Configure a
dedicated namespace in the chart values to separate access to those credentials.

The 0.3.x operator accepts both previous and current CRD spec field names. The
previous names are deprecated. Set only one name per field; setting both is
rejected by the API schema. The `grid-site` chart translates previous value
names into current CRD fields and also rejects both names together. See the
[0.3.x CRD field migration guide](../installation/migration-0.3.x.md)
for the planned removal and upgrade steps.

## GridNetwork

The AGN logical network and top-level tenancy scope. `GridNetwork` is a
cluster-scoped CRD. The current AGN deployment supports only one
`GridNetwork` in the cluster. The operator fails startup when it finds more
than one; adding another after startup is not supported.

It carries this cluster's own conditions. `Accepted`, `ResolvedRefs`, `Enrolled` and
`IdentityValid` gate its roll-up, in that order. `RotationOnSchedule` `False`, a
`PeerTimeValid` of `False`, or a `GatewayAgesRejected` of `True` sets `Degraded` with
that condition's reason, and after them, under Local membership, a `False`
`PeerSignalsReachable`, `PeerCatalogsAccepted`, `PeerMembership` or `PeerPollsComplete`.
None of the peer conditions gates `Available`, and a site switched to Swim drops them. Each condition type has one writer, and the controller alone
writes `Available`, `Progressing` and `Degraded`. Every reason is listed in
[Site Enrollment](../installation/enrollment.md#3-enroll-the-site).

`spec.gridId` is the grid the network belongs to, set once: a DNS label of at most 63
characters, and a change is refused. `status.gridId` mirrors it. Bootstrap owns
ID generation and persists it independently of the CA; the operator never mints
an ID. Every installation waits with `Accepted=False`, reason `GridIdUnset`,
until `spec.gridId` is set. During migration, bootstrap adopts an existing
declared or legacy status ID rather than creating another one. Moving a cluster
to another grid means deleting and recreating its GridNetwork, and its identity with it.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: production
spec:
  gridId: "production-grid"     # the stable ID supplied by bootstrap
  seeds:
    - "10.0.0.5:7946"
  consumerGateways:
    - name: inference-gw
      namespace: praxis-system
      siteName: cluster-east   # optional; defaults to network name
      modelDiscoveryContractVersion: v1 # only after the running gateway implements the model-discovery contract
      praxisConfig:               # optional; opt-in consumer Praxis config generation
        generate: true
        enableProjectedCredentials: true  # generate filter, then roll out consumer
        supportsProjectedCredentials: true # set only after that rollout
        credentialMountPath: /run/secrets/grid-credentials
        configMapName: praxis-consumer-config
        tlsCertMountPath: /etc/praxis/tls
        mountReconciliation:
          enabled: true
          deploymentName: inference-gw
          containerName: praxis
        clusterEndpoints:           # endpoint topology for load_balancer
          - cluster: site-a
            address: "10.0.0.4:30080"
            transport:
              mode: mutual_tls         # mTLS with CA verification and client cert
              sni: site-a.grid.internal
          - cluster: api-provider
            address: "api.example.internal:443"
            transport:
              mode: tls                # verified server TLS
              sni: api.example.internal
              caSecretRef:
                name: api-provider-ca  # Secret in praxis-system (gateway namespace)
                # key: ca.crt          # optional; defaults to ca.crt
  region: us-east-1
  zone: us-east-1a
  swim:
    probeInterval: 5s           # WAN probe interval
    suspicionTimeout: 10s       # before declaring dead
    gossipNodes: 3              # indirect probe fanout
  peerTrust:
    mode: pin                    # pin (default) or spiffe
    signedState: "off"           # off (default) or enforce
  tls:
    caSecretRef:
      name: grid-ca
      namespace: praxis-system
    siteSecretRef:
      name: grid-site-cert
      namespace: praxis-system
    swimKeySecretRef:
      name: swim-key
      namespace: praxis-system
  budgetPolicy:                   # optional; absent means no tenants are tracked
    tenants:
      - tenantId: tenant-a
        capUsd: 100.0
      - tenantId: tenant-b
        capUsd: 250.0
```

`peerTrust.mode` controls how this grid accepts peer signing identities for
SWIM state gossip. `pin` (the default) requires the peer certificate's
canonical DER SHA-256 fingerprint to appear in that peer's
`GridSite.spec.trust.canonicalFingerprints`. It still verifies the certificate
against this grid's CA and expected site identity. `spiffe` also verifies the
grid CA and site identity, requires the site's SPIFFE URI, and does not read
fingerprints. See [Signed gossip trust](signed-gossip-trust.md) for how those
identities gate signed CRDT state and how the certificate bootstrap works.

`peerTrust.signedState` gates that signing. `off` (the default) gossips state
unsigned, as before. `enforce` signs this site's state and refuses peer state
without a verified signature. Switch every site together: an enforcing site
drops state from peers still at `off`. Setting `off` again removes the signer
and pins on the next reconcile.

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

**Status fields**: `gridId`, `connectedSites`, `remoteProviderCount`,
`observedGeneration`, `phase`, `conditions[]`, `praxisConfigStatus[]`,
`mountReconciliationStatus[]`, `budgetStatus[]`, `gatewayAgeReports[]`,
`gatewayAgeReportOverflow`

**Conditions**: `status.conditions` carries a `PeerTimeValid` condition,
written by the operator's peer poller under its own field manager
(`grid-operator-peer-signals`), never by `GridNetwork` reconciliation. `True`
means no polled peer has a rejected timestamp; `False` means at least one does.

| Status | Reason | When |
|---|---|---|
| `False` | `TimestampOutOfRange` | A polled peer's latest answer carried a row stamped more than a second past its `Date`, or more than a day before it. The message names those peers (the first ten, then a count): check their clocks. |
| `True` | `PeerTimestampsValid` | No polled peer's latest answer was rejected for its age. |

Under Local membership the poller also writes three conditions, each under its own field
manager. Which peers, and why, is on each peer's `GridSite`, as described in
[Peer conditions under Local membership](#peer-conditions-under-local-membership).

`PeerSignalsReachable` (`grid-operator-peer-reach`) counts the peers whose signals
endpoint did not answer. A peer not yet checked, an egress-only peer, and a peer the
round had no time for are not counted.

| Status | Reason | When |
|---|---|---|
| `False` | `PeersUnreachable` | A checked peer's signals endpoint did not answer. The message counts them, for example `2 of 5 peers unreachable`. |
| `True` | `PeerSignalsReachable` | Every polled peer's signals endpoint answered. |

`PeerCatalogsAccepted` (`grid-operator-peer-catalogs`) counts the peers whose catalog,
or part of it, was refused, out of the peers whose catalog is held.

| Status | Reason | When |
|---|---|---|
| `False` | `CatalogsRefused` | A peer's catalog was refused. The message counts them, for example `1 of 5 peers' catalogs refused`. |
| `True` | `PeerCatalogsAccepted` | Every peer catalog read is accepted. |

`PeerPollsComplete` (`grid-operator-peer-polls`) says whether each round had time to poll
every peer. A peer the round had no time for keeps its last `SignalsReachable` verdict, so
the overrun is this site's, never the peer's.

| Status | Reason | When |
|---|---|---|
| `False` | `PollRoundOverrun` | The last round ran out before it polled some peers. The message counts them, for example `2 of 40 peers not polled within the round`. |
| `True` | `PeerPollsComplete` | The last round polled every peer. |

A `Swim` condition, under its own field manager (`grid-operator-swim`), reads `True`
with reason `Swim` while SWIM runs, and `False` when it is configured but cannot start,
with a reason naming the cause and the detail in the message. The operator
keeps running without membership gossip, rather than exiting into a restart loop. It
restarts itself once a late advertise address appears, and 300 s after a failure that
may clear on another try: `AdvertiseAddressUnresolved`, `RevisionLeaseFailed` or
`StartFailed`. A setting that is wrong waits for a rollout that fixes it. Under Local
membership the operator removes the condition, since SWIM never runs there.

| Status | Reason | When |
|---|---|---|
| `False` | `AdvertiseAddressPending` | No advertise address within 180 s; the operator restarts once one appears. |
| `False` | `AdvertiseAddressUnresolved` | The advertise address could not be resolved; retried after 300 s. |
| `False` | `AdvertiseAddressUnspecified` | The address to advertise is unspecified; set `GRID_SWIM_ADVERTISE_ADDR`. |
| `False` | `BindAddressInvalid` | `GRID_SWIM_BIND_ADDR` is not a socket address. |
| `False` | `KeyInvalid` | `GRID_SWIM_ENCRYPT_KEY` does not parse. |
| `False` | `RevisionLeaseFailed` | The signal revision lease could not be reserved; retried after 300 s. |
| `False` | `StartFailed` | The runtime failed to start, binding its socket included; retried after 300 s. |
| `True` | `Swim` | SWIM runs. |

**Membership mode:** `spec.membership.mode` selects how a site learns its
members. `Swim` discovers members by SWIM gossip. `Local` uses the site's
declared `GridSite` objects and requires `spec.signalTransport.mode: poll`:

```yaml
spec:
  membership:
    mode: Local
  signalTransport:
    mode: poll
```

An explicit mode overrides the install's `GRID_MEMBERSHIP_SOURCE`. When the
field is absent, the install setting applies, defaulting to `Swim`. Changing
the mode, including removing an override, gracefully restarts the operator
to apply the new selection. A declared `Local` mode disables SWIM even when
the platform Deployment still sets `GRID_SWIM_BIND_ADDR`; the Deployment
does not need to change. Local requires reachable peer signals endpoints
and matching identities, independently of how those resources are delivered.

The `grid-operator` and `grid-site` charts write this field from their existing
`grid.membership.source` value. Under `Local` they also default signals to
poll and reject contradictory gossip settings. Install-only Local mode
continues to reject an explicitly configured SWIM bind address.

Local assumes something outside the grid delivers every site's `GridSite` records, the
same set to every member. The operator discovers no peers in this mode. It polls the
sites its own configuration lists, so a member missing from a site's records is a member
that site never reaches.
[Open Cluster Management](https://open-cluster-management.io/) is the reference example,
placing the records on each cluster in a cluster set. Argo CD or plain GitOps deliver the
same records just as well. Red Hat Advanced Cluster Management is the product built on
Open Cluster Management.

A `PeerMembership` condition, under its own field manager
(`grid-operator-peer-membership`), compares the `X-Grid-Membership` header each
polled peer answers with against this site's `GRID_MEMBERSHIP_SOURCE`. A peer that sends
no header is an older release and is judged by nothing. A grid that mixes sources is
not supported: polling goes on so the direction that works still routes, but a
Swim site never reads a Local site's catalog, and the condition stays False
until every site names one source.

| Status | Reason | When |
|---|---|---|
| `False` | `PeerMembershipMismatch` | A polled peer names a different membership source. The message names those peers and their sources: set `grid.membership.source` alike on every site. |
| `True` | `PeerMembership` | Every polled peer that names a mode names this site's. |

A relayed age survives a clock step on every hop, so a step alone never makes
this condition false: it points at a peer whose clock or stamping is wrong.
Rejected rows are dropped and counted in
`grid_peer_signals_refused_total{peer,reason="skew"}`. Only rejected rows become
unavailable for provider measurement; valid rows from the same peer remain
available. A peer that does not answer keeps its standing,
a peer that leaves membership is dropped, and one clean answer restores its
validity. The operator conservatively preserves an existing `False` condition
after restarting until every currently polled peer has answered or left
membership. Unavailable secure polling is not a clean round and does not
establish recovery. The condition is written only when its status, its
message or the generation changes, since each write starts a `GridNetwork`
reconcile.

Peer-age observation and status publication wait for the operator's existing
membership-convergence gate, so an initial empty snapshot cannot erase a live
rejection during restart. Signal collection continues while this gate is held.
A round begun while held cannot publish even if convergence finishes during
collection; the next round uses the converged membership.

`remoteProviderCount` reflects the number of remote `InferenceProvider`
records received from peer sites via CRDT broadcast.  Local providers and records
from other `GridNetwork`s are excluded from the count.

### Gateway age evidence

`conditions[]` is a map keyed by condition type. `GatewayAgesRejected` reports
local gateway ingest rejection under its own status manager, independently of
peer diagnostics and normal GridNetwork reconciliation. `True` means a fresh
gateway process has unresolved rejected ages. `False` requires genuine newer
clean evidence from every recorded rejecting process. `Unknown` means the
remaining evidence is stale, predates the operator restart, or reporter
capacity was exceeded. Conditions never change network phase or routing.

`gatewayAgeReports[]` retains at most 128 records keyed by random process epoch:
replica label, sequence, cumulative skew count, original report flag, effective
rejection state, and receipt timestamp. Live freshness uses a five-minute
monotonic receipt window. Expiration is evaluated when another report arrives;
if reporting stops entirely, the last condition remains until a report arrives.
After an operator restart, persisted rejected epochs have unknown freshness
until that same epoch submits a strictly newer observation. Wall-clock changes
and duplicate reports cannot establish recovery or renew freshness.

A clean report from another replica or replacement process never clears an
older rejected epoch. The listener identifies the site, not authoritative pod
membership. Once an operator has independently verified that an old process
retired, they may explicitly remove its `gatewayAgeReports` status entry.
The next report recomputes the condition. Silence or a timeout is insufficient
proof of retirement.

Only clean bookkeeping may be evicted to admit a new process. When all 128
entries hold unresolved evidence, the operator persists
`gatewayAgeReportOverflow: true` and refuses new identities; existing processes
can still report recovery. This bounded uncertainty survives recovery and
restart. Clear it explicitly only after verifying reporter coverage. Inspect
status with `kubectl get gridnetwork NAME -o yaml`; status edits require RBAC
permission for the GridNetwork status subresource.

`praxisConfigStatus[]` is populated for each gateway with
`praxisConfig.generate: true`, reporting the outcome of the most recent
render/apply attempt.

Set `praxisConfig.mountReconciliation.enabled: true` to delegate generated
Secret mounts and coordinated Deployment rollouts to Grid. This requires an
explicitly annotated Deployment in `GatewayRef.namespace`, and a Role in that
namespace letting the operator `get` and `patch` that one Deployment; the
praxis-gateway chart renders it, see
[Deployment access](operations.md#deployment-access). The operator manages
only its reserved projected volumes and the selected container's mounts. Secret
references must be in the gateway namespace. `praxisConfigStatus[].phase` of
`Rendered` describes the ConfigMap; only
`mountReconciliationStatus[].phase: Ready` reports a ready gateway revision.
With delegation disabled, the operator still publishes a reference-only
`grid-mount-requirements-<hash>` ConfigMap with the document under
`mount-requirements.json` for the gateway owner to consume.

`clusterEndpoints[].transport.mode` accepts `mutual_tls`, `tls`, or `plaintext`.
The optional `transport.caSecretRef` is valid for `tls` and names a custom CA
Secret in the target `GatewayRef.namespace`; it has `name` and optional `key`
fields only, with the key defaulting to `ca.crt`. Existing manifests must remove
its former `namespace` field and place the Secret in the gateway namespace.
CRD pruning removes unknown fields: `Warn` mode accepts the object and reports a
warning, `Ignore` silently drops the field, and `Strict` rejects the request.
This namespace-local reference is distinct from `spec.tls.caSecretRef`, which
retains its explicit `namespace`. `mutual_tls` uses `spec.tls.caSecretRef` and
`spec.tls.siteSecretRef` and does not accept a custom CA override.

### Tenant budget tracking

`budgetPolicy.tenants[]` opts individual tenants into cumulative spend
tracking. AGN merges each site's locally recorded spend for a tenant into a
per-tenant CRDT counter (a `GCounter`, one slot per originating site) that is
gossiped over SWIM alongside provider state, so the reported total reflects
spend recorded anywhere in the grid, not just the local site.

For every tenant declared in `budgetPolicy`, `budgetStatus[]` reports:

- `tenantId`: matches `budgetPolicy.tenants[].tenantId`
- `capUsd`: copied from the policy, in USD
- `spendUsd`: the converged cross-site total, in USD
- `spendRatio`: `spendUsd / capUsd`, for at-a-glance dashboarding

`budgetStatus[]` is a status **signal only**. AGN does not itself degrade or
reject traffic when a tenant's `spendRatio` reaches or exceeds `1.0`; that
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
| `reason` | string | Machine-readable reason (`MissingClusterEndpoint`, `PraxisConfigRenderFailed`, `PraxisConfigApplyFailed`); empty when `Rendered` |
| `message` | string | Human-readable diagnostic; never contains token bytes |
| `observedGeneration` | integer | `GridNetwork` generation when this entry was last updated |

For a delegated gateway, `mountReconciliationStatus[]` reports the rendered
requirements revision, the config revision last rolled out, the Deployment
generation, and one of `RequirementsRendered`, `WaitingForSecret`,
`MountsReconciling`, `WaitingForRollout`, `Ready`, or `Error`. Messages contain
resource names and paths only. They never contain Secret data or private keys.

Example status output:

```yaml
status:
  phase: Active
  gridId: grid-abc123
  connectedSites: 2
  praxisConfigStatus:
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
      reason: PraxisConfigRenderFailed
      message: "consumer config render: overlay local_site must not be blank"
      observedGeneration: 7
```

### CRD-driven SWIM seeds

`spec.seeds` is a list of SWIM endpoints (`host:port`) used to bootstrap
SWIM mesh formation. Each entry may be a literal IPv4 address, a bracketed IPv6
address, or a DNS hostname. Hostnames are resolved with a bounded lookup before
they are announced to the running SWIM runtime on every
`GridNetwork` reconcile.  Re-announcing to an existing peer is idempotent: foca
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
is process-global: all `GridNetwork` resources in the operator process share the
same SWIM node.  Seeds from any `GridNetwork` reach the shared SWIM membership
table.  Provider CRDT state remains scoped per network.

**Self-filtering:** The operator removes its own SWIM bind address from
`spec.seeds` before announcing, preventing self-join loops.

**`spec.tls.swimKeySecretRef`:** References a Kubernetes Secret containing the 32-byte
AES-256-GCM key for SWIM transport authentication.  When configured, the
`GridNetwork` controller reads the key from the Secret and configures the
SWIM runtime to encrypt all outgoing UDP packets and reject incoming packets
that fail authentication before it announces CRD seeds or publishes
certificate/provider state for that reconcile.

The Secret must contain a `"key"` field (or the field named by `swimKeySecretRef.key`)
with exactly 32 bytes.  If the Secret is absent, unreadable, or has the wrong
length, the reconcile fails before CRD seed announcement and state broadcast.
The process-global SWIM runtime keeps any previously loaded key until restart;
it does not switch to plaintext for that configured reconcile.

For local development and testing, the `GRID_SWIM_ENCRYPT_KEY` environment
variable (64-character hex) provides an alternative key injection path without
requiring a Kubernetes Secret.  Because environment variables are visible to
same-host process inspectors, this is not the production Secret delivery path.

Routing eligibility remains gated separately by `GridSite.status.phase == Active`,
or `Unreachable` under poll signal transport,
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

Under poll signal transport the operator removes no candidate for liveness. A failed
probe, a Dead or Suspect SWIM member, this TTL and the SWIM dead-member TTL leave a
remote site's candidates in place, and the gateway routes around a silent site. A
candidate leaves when trust fails, its endpoint changed and awaits a probe, its
providers are withdrawn, or its GridSite is deleted.

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

### GatewayRef.providerHopEndpoints

`spec.consumerGateways[].providerHopEndpoints` independently configures the
provider-hop trust allowlist used by the embedded `grid-gateway`. Each entry
names a provider cluster and declares its verified mTLS mode and SNI. The
embedded gateway's startup upstream configuration must use that TLS identity.
This field is independent of `praxisConfig`; absent or disabled generated
consumer Praxis config cannot make embedded serving fail validation or change
its provider-hop allowlist.

### GatewayRef.modelDiscoveryContractVersion

Set `spec.consumerGateways[].modelDiscoveryContractVersion: v1` only after the
running gateway image understands and enforces the `expires_at` field on local
discovered-model candidates. Missing or unknown versions cause Grid to omit
discovered candidates for that gateway while keeping static candidates.

### GatewayRef.praxisConfig

`spec.consumerGateways[].praxisConfig` opts a gateway into operator-managed consumer
Praxis `ConfigMap` generation.

| Field | Default | Meaning |
|---|---|---|
| `generate` | `false` | Set to `true` to enable consumer config generation for this gateway. |
| `enableProjectedCredentials` | `false` | Generate the dynamic `credential_inject` filter and projected mount root, including with an empty candidate set. Roll out the consumer after enabling this. |
| `supportsProjectedCredentials` | `false` | Attest that the consumer has loaded that filter/config and mounted its referenced Secrets. Until true, credential-bearing overlay revisions are retained with `ProjectedCredentialsUnsupported`. |
| `credentialMountPath` | `/run/secrets/grid-credentials` | Base directory for projected credentials. In projected mode, mount each Secret at `{base}/{namespace}/{name}` with Secret keys as files. Static `file:` entries continue using their explicitly configured paths. |
| `configMapName` | `praxis-consumer-config` | Name of the generated `ConfigMap` in the gateway namespace. |
| `clusterEndpoints[]` | `[]` | Endpoint topology for `load_balancer` clusters. Each entry maps a candidate cluster name to an address with explicit `transport` configuration. Missing transport fails closed. |
| `clusterEndpoints[].transport.mode` | _(required)_ | `mutual_tls` (CA/client cert/SNI/verify), `tls` (server-authenticated TLS), or `plaintext` (no TLS, insecure/dev-only). |
| `clusterEndpoints[].transport.sni` | _(required for TLS modes)_ | TLS Server Name Indication; must match the provider certificate SAN. |
| `clusterEndpoints[].transport.caSecretRef` | omitted | Optional CA Secret for `tls`, resolved in `GatewayRef.namespace`; `name` required, `key` defaults to `ca.crt`, and `namespace` is not a field. |
| `tlsCertMountPath` | `/etc/praxis/tls` | Base path for mounted TLS files used when a `clusterEndpoints[]` entry uses `mutual_tls` transport. |
| `listenerPort` | `8080` | HTTP port for the generated `listeners[0].address` (`0.0.0.0:{listenerPort}`). |

Grid publishes a valid empty versioned overlay when no candidates remain.
Every consumer of that overlay must use a Praxis AI image that accepts empty
versioned snapshots; older images, including the chart-default 0.4.0 image,
are incompatible. Upgrade and roll out the consumers before deploying this
Grid version. Malformed or undeliverable updates still retain the last valid
revision; an authoritative empty candidate set does not.

When `generate: true`, the `GridNetwork` controller renders a `praxis.yaml`-keyed
`ConfigMap` in the gateway namespace on each reconcile.  The generated config is a
complete, runnable Praxis config containing:

- `listeners:` with one public listener at `0.0.0.0:{listenerPort}`
- `filter_chains:` holding the consumer chain with:
  - `intelligent_route` reading the versioned routing overlay with expected
    network, gateway, namespace, and local-site scope. Candidates and selection
    policy are not copied into startup-only YAML.
  - `credential_inject` is included when current candidates have credential
    references or `enableProjectedCredentials: true`. In projected mode it
    starts with an empty table and resolves later references under the
    projected Secret mount; missing files fail closed. Token bytes are never
    written to the `ConfigMap`.
  - `load_balancer` entries for every configured endpoint, including currently
    inactive providers needed for restoration. Every potentially routable
    cluster must have a matching `clusterEndpoints[]` entry with endpoint address
    and explicit `transport` configuration.  `transport.mode` is the security
    switch, not `sni` presence.  Missing transport fails closed
- `admin:` with a health and metrics listener at `127.0.0.1:9901` under `metrics_address`
- `shutdown_timeout_secs: 5`

The generated credential-injection config is for credentials intentionally
projected into the consumer gateway's namespace. For remote provider sites,
credentials should normally be mounted only in the remote site or provider-side
component that makes the final backend call.

The `credential_inject` filter is a Praxis AI runtime dependency. The AGN
operator can render the config shape, but the deployed Praxis AI image must
include that filter for the generated config to start successfully.

The consumer Deployment must mount the gateway's `grid-overlay-<network>-<gateway>`
ConfigMap key `routing-overlay.json` at `/etc/praxis/routing/routing-overlay.json`
as a projected volume (never with `subPath`). Cross-cluster consumers must
deliver both the Praxis config ConfigMap and routing overlay to the consumer
cluster. The initial migration from static inline candidates requires a
consumer rollout; listener, endpoint/TLS, or credential-table changes continue
to require a rollout, while route-only overlay revisions hot reload.

For generated consumer configs, first set `enableProjectedCredentials: true`
and roll out the consumer so it loads the filter and mount root. Then set
`supportsProjectedCredentials: true` to attest that rollout and mount the
referenced Secret. The Grid controller does not own or restart consumer
Deployments. Until readiness is attested, credential-bearing overlay revisions
are retained. In projected mode, the generated config uses an empty credential
table and resolves the selected Secret reference at request time, failing
closed when it cannot read it.

When `generate: false` or `praxisConfig` is absent, no generated consumer
Praxis `ConfigMap` is applied. The routing overlay remains active, and the
embedded gateway's serving config is controlled separately by
`GatewayRef.providerHopEndpoints`.

## GridSite

Represents a site in the grid. A peer is created manually for seed peers or
automatically by SWIM discovery. This cluster's own site is the GridSite named
after its identity, and before it has one, the GridSite named in the operator config.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridSite
metadata:
  name: cluster-b
  labels:
    grid.praxis.fast/network: production
spec:
  gridNetworkRef: production
  gatewayEndpoint:
    address: egress.cluster-b.example.com:8443
    tls:
      mode: Mutual
      serverName: cluster-b.grid.internal
  trust:
    canonicalFingerprints:
      - "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
  region: us-east-1
  zone: us-east-1a
  sovereigntyZone: us
```

**Phases**: Pending → Discovered → Connecting → Active → Unreachable → Left

**Status fields**: `conditions`, `phase`, `reason`, `message`, `observedGeneration`,
`publicCertPem`, `capabilities` (inference, agentTools, agentToAgent),
`lastProbeTime`, `lastTransitionTime`, `absentSince` (auto-discovered sites only).
Status is membership only, each cluster's view of one site, this cluster's own site
included. The own site's identity conditions live on the GridNetwork.

### This cluster's own site

This cluster's own GridSite is declared like any peer, by the grid-site chart, by
`grid.id` on the grid-operator chart, by GitOps or by hand: named after the site the
identity names and labelled `grid.praxis.fast/provider-site`. If nothing declares one,
the operator creates it from the identity, as it does the GridNetwork, under
`--grid-create-local-records`, on by default. It never overwrites a GridSite something
else declared, and enrollment does not wait for it. The own GridSite carries no local
config. How this site enrolls and renews lives in
the operator config: the ConfigMap `grid-operator-config` in the operator namespace,
key `config.yaml`, which the grid-operator chart renders from its `enrollment` values
(`operatorConfig.name` renames it). The operator reads it through the API and watches
it, so an edit applies without a restart. It holds only this operator's or this site's
own settings. Grid-wide settings stay on the GridNetwork, and credentials stay in
Secrets it names.

```yaml
site:
  name: east2
  signerEndpoint:
    url: https://enrollment.apps.example.com
    caBundleRef: {kind: Secret, name: grid-ca-bundle, key: ca.crt}
    gridCaBundleRef: {kind: Secret, name: grid-ca-anchor, key: ca.crt}
    rotation: Enabled
  inviteSecretRef: {name: grid-invite-east2, key: token}
```

| Field | Meaning |
|---|---|
| `site` | This cluster's own site. Optional, like every section. Unset, the site does not enroll itself. |
| `site.name` | The site name, and the name of its GridSite. Required. The invite label and the issued certificate must match it. |
| `site.signerEndpoint.url` | The base URL of the signer the site enrolls and renews through. https, naming a host with an optional numeric port, without credentials, query or fragment, and not a host under `grid.internal`. |
| `site.signerEndpoint.caBundleRef` | A `Secret` in the operator namespace pinning the signer to enroll, and by default the grid CA it returns. `kind` is required, and `Secret` is the only kind, so whoever can edit this config cannot pin a CA of their own. Required with `inviteSecretRef`. Renewal trusts the grid CA the site holds together with this bundle. Pin the issuing intermediate or the signer's serving certificate, never a public root. The object must carry `grid.praxis.fast/signer-bundle=true`, as must the `gridCaBundleRef` object. |
| `site.signerEndpoint.gridCaBundleRef` | The grid CA the returned CA must match: a bootstrap override for a signer that cannot serve its own bundle, such as one behind a certificate from another CA. |
| `site.signerEndpoint.rotation` | Whether this operator renews the identity through the signer. `Enabled` (default) renews when a third of its lifetime remains, and is refused under pin peer trust with `Accepted` `False` and `RotationUnsupportedUnderPinTrust`. `Disabled` uses the signer to enroll only. Enrollment runs whatever this says. |
| `site.inviteSecretRef` | The Secret holding the one-time invite token, an opaque credential the signer issued, read once while the site has no identity. `key` defaults to `token`. The Secret must carry `grid.praxis.fast/site=<site.name>`. |

An optional top-level `version` says which document this is. Absent reads as `1`, the
only version this operator reads, and any other refuses the document. A top-level
section the operator does not read is ignored and the rest applies, so an operator
older than a new section keeps running through a rollout. It logs the section, records
an `OperatorConfigSectionIgnored` Warning Event on the ConfigMap and counts it in
`grid_operator_config_ignored_sections`. Inside a section it reads, the operator refuses
the whole document: an unknown key, a bad URL or a missing field reports `Accepted`
`False` with `OperatorConfigInvalid` on the GridNetwork, the message naming the field,
and no section of it is acted on. A `site.name` other than the site the held identity
names reports `SiteNameMismatch`. Before the GridNetwork exists, the operator sends
nothing, logs why and records an `AwaitingGridNetwork` Event on the ConfigMap, once. Under pin peer trust,
`rotation: Enabled` reports `Accepted` `False` with `RotationUnsupportedUnderPinTrust`
and nothing renews. Every condition and reason is listed in
[Site Enrollment](../installation/enrollment.md#3-enroll-the-site). A local site may
exist before its GridNetwork, and the GridSite controller waits for the network
without reporting an error.

The operator self-signs a grid CA and site identity for a GridNetwork that names TLS
Secrets neither of which exists. It never does while the operator config names this
site, or is refused, since enrollment writes both. It waits 30 seconds after the
GridNetwork is created, so an operator config applied with it is seen first.

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
| `Connecting` | Gateway address known (`spec.gatewayEndpoint.address` non-empty) | `GridSite` controller advances from Discovered, or from Pending for a declared site with both endpoints; performs identity-aware probe |
| `Active` | `TlsVerified` | `GridSite` controller promotes from Connecting only after identity-verified TLS succeeds |
| `Unreachable` | Connectivity failure while Active | `GridSite` controller moves Active → Unreachable when the endpoint cannot be reached |
| `Left` | Set on graceful site departure | Preserved by operator once set |

**Reason codes** (in `status.reason`):

| Reason | Phase | Meaning |
|---|---|---|
| `AwaitingDiscovery` | Pending | Site record exists; SWIM has not yet observed the peer as Alive |
| `SWIMDiscovered` | Discovered | Peer observed as Alive in SWIM membership; gateway address propagating |
| `GatewayAddressKnown` | Connecting | Gateway address received; advancing to Connecting |
| `EndpointsDeclared` | Connecting | Declared site names a dialable gateway endpoint, which also gives its signals endpoint; advancing without discovery |
| `GatewayAddressMissing` | Discovered, Connecting, or Unreachable | No gateway address known (see `GRID_GATEWAY_ADDRESS`), or a previously probed site lost it |
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
- Pending → Connecting: the `GridSite` controller advances a declared site (one without
  the `grid.praxis.fast/auto-discovered` label) that sets a dialable
  `spec.gatewayEndpoint.address`, so it does not wait on SWIM.
- Discovered → Connecting: the `GridSite` controller advances automatically when
  `spec.gatewayEndpoint.address` is non-empty. For auto-discovered sites, the egress address comes from
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
`grid_site_phase{phase="Active"} == 1`. It follows the phase the wide PHASE column shows,
updates on every `GridNetwork` reconcile, and drops a site's series once its `GridSite`
is gone. Cardinality is six series per site. `grid_site_phase_transition_total`
still counts the transitions by phase and reason.

**`spec.gatewayEndpoint.address` source:** For auto-discovered sites, the egress address is sourced from
the remote operator's `GRID_GATEWAY_ADDRESS` environment variable, propagated through the SWIM
state broadcast.  If the remote operator has not configured `GRID_GATEWAY_ADDRESS`, the field
is empty and the site stays Discovered.  For manually-applied `GridSite` resources, set
`spec.gatewayEndpoint.address` explicitly to the data-plane gateway endpoint.

**`status.publicCertPem`:** The public site certificate PEM received from the remote site via
SWIM state broadcast.  Before storage, the operator performs a structural check:
private-key markers (`PRIVATE KEY`) cause the input to be discarded entirely and an error
logged.  Non-certificate PEM triggers `TrustMaterialInvalid` status.  A valid `CERTIFICATE`
header passes the structural check.

This field contains only the public certificate, never a private key.  A non-empty
`publicCertPem` means the remote site has shared its public identity material and the
structural check passed.  It does **not** mean:

- The certificate has been chain-verified against a trusted CA.
- The peer is authenticated or authorized for routing.
- The content has been parsed as X.509.

Private keys, bearer tokens, provider credentials, and Kubernetes Secret contents must never
be written to status.

**Signals endpoint:** under Local membership a site serves its signals by default at its
gateway endpoint's host, under the TLS server name `<site>-signals.grid.internal`, where `<site>` is
the GridSite name. The port is the gateway's own, for a site whose one router selects the
signals listener by server name, such as an OpenShift passthrough Route. The operator flag
`GRID_SIGNALS_PEER_ENDPOINT_PORT` (chart `signals.peerEndpointPort`) sets one port for the
grid instead, for sites whose one load balancer carries both. A site whose signals sit
elsewhere sets the optional `spec.signalsEndpoint`, and each field it sets replaces the
derived one. Loopback, link-local, and cloud metadata hosts are not dialled.

**`spec.signalsEndpoint` fields**, all optional:

| Field | Meaning |
|---|---|
| `address` | Signals listener `host:port`. Absent, the gateway endpoint's host |
| `serverName` | TLS server name sent to the listener. Absent, `<site>-signals.grid.internal` |

The address carries the port. Without one, the gateway endpoint's host is dialled at the
operator's signals port setting, or at the gateway's own port. The poller verifies the peer
by pin or SPIFFE ID, so the server name need not appear in the peer's certificate.

**`spec.gatewayEndpoint.tls` fields:**

| Field | Meaning |
|---|---|
| `mode` | `Mutual` (default): TLS handshake with CA verification and client auth; `Plaintext`: TCP-only diagnostics that never become routing-eligible |
| `serverName` | Expected DNS identity for TLS SNI and SAN verification; required for `Mutual`, must be absent for `Plaintext` |

**`spec.trust` fields:**

| Field | Meaning |
|---|---|
| `canonicalFingerprints` | Required DER-certificate SHA-256 pins (`hex(sha256(der_bytes))`), one or two entries for bounded rotation overlap |
| `certFingerprint` | **Deprecated.** Legacy PEM-based fingerprint; rejected at runtime with `TrustMaterialInvalid`; migrate to `canonicalFingerprints` |

The same `GridSite.spec.trust.canonicalFingerprints` policy is used by
`peerTrust.mode: pin` to verify a site's signing identity for SWIM/CRDT state.
The certificate must also chain to the grid CA and identify the expected site;
the fingerprint is an additional exact-certificate check.

**Routing eligibility:** `GridSite.status.phase == Active` is the control-plane eligibility
gate for remote CRDT provider records. Active means the control plane has
verified the remote site's identity through TLS, which is sufficient to include
the site's providers in routing overlays for consideration.

Provider records from a peer whose `GridSite` is in `Discovered`, `Connecting`, `Pending`,
or `Left` are excluded from the routing overlay, and so are those from an `Unreachable`
peer except under poll signal transport, where the gateway judges reachability from
signals. Records from a peer with no matching `GridSite` are also excluded (fail-closed).

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
and its mTLS gateway probe (`spec.gatewayEndpoint` + `spec.trust`) apply to reaching
*remote* sites, or a manually-configured peer gateway endpoint. See
[Architecture Overview -> Single-Site and Combined Deployments](overview.md#single-site-and-combined-deployments).

See [Routing eligibility](routing.md#routing-eligibility) for the full gating rule.

### Peer conditions under Local membership

Under Local membership the lease holder writes two conditions on each peer's
`GridSite`, each under its own field manager. They are this site's view of the peer.
Neither gates the phase or `Available`. A `False` one raises the peer's `Degraded`,
`SignalsReachable` first, then `CatalogAccepted`. `Degraded` copies its reason, and its
message names the condition. The GridSite controller writes `Degraded` on the reconcile
that the poller's write triggers.

`SignalsReachable` (`grid-operator-peer-reach`) says whether the peer's signals endpoint
answered the last poll. The message carries the polled address and the detail.

| Status | Reason | When |
|---|---|---|
| `True` | `SignalsReachable` | The peer's signals endpoint answered. |
| `Unknown` | `AwaitingFirstCheck` | The peer has not yet answered or failed a poll. |
| `False` | `NotFound` | It answered 404: check its signals endpoint and port. |
| `False` | `Timeout` | It did not answer in time. |
| `False` | `TLSRefused` | The TLS handshake or the peer's identity was refused. |
| `False` | `ConnectionRefused` | The connection was refused or never established. |
| `False` | `RequestFailed` | Any other failure, named in the message. |
| `False` | `EgressOnly` | The peer declares no signals endpoint, so it is not polled: set `spec.signalsEndpoint`, or a dialable `spec.gatewayEndpoint`, to poll it. It raises neither the peer's `Degraded` nor the `GridNetwork` count. |

`CatalogAccepted` (`grid-operator-peer-catalogs`) says whether the peer's catalog was
accepted in the last rendering. It is written once a catalog is held.

| Status | Reason | When |
|---|---|---|
| `True` | `CatalogAccepted` | The peer's catalog is accepted, in full or up to what it may contribute. Each declared peer is owed the overlay's 1024-candidate ceiling divided by the peers, one candidate per model; what is left once every peer has its share goes to the peers over theirs in the order their catalogs first arrived. A peer is never refused for its size: past what it may contribute, the rest of its catalog waits, models in name order, and the message counts how many of its candidates wait and names who took the rest. The message also names what the peer holds that another peer also names, and the peers it leaves short by taking the remainder. |
| `False` | `OwnCluster` | A record names one of this site's own clusters. |
| `False` | `HopClaimed` | A record names a cluster bound to another site's gateway hop. The message names that site. |
| `False` | `ClusterClaimed` | A record names a cluster another peer holds by declaration: the cluster is bound to that peer's gateway hop, or carries that peer's name. The message names the holder. |
| `False` | `ClusterContested` | A record names a cluster another peer also names, and nothing declares whose it is, so no peer routes it until the names differ. The message names the other peers. |

A peer that leaves the Local peer set loses both conditions on the next round, and an
operator that starts under Swim removes them once. Either way the peer's `Degraded`
returns to `False` on its next reconcile.

Example status (Mutual TLS verified):

```yaml
status:
  phase: Active
  reason: TlsVerified
  message: "TLS handshake succeeded; certificate chain, identity, and pin verified"
  observedGeneration: 5
  lastProbeTime: "2026-07-30T12:00:00Z"
  lastTransitionTime: "2026-07-30T11:55:00Z"
```

Example status (trust material missing):

```yaml
status:
  phase: Connecting
  reason: TrustMaterialMissing
  message: "required trust material not available"
  observedGeneration: 3
```

Example status (gateway address not configured on remote operator):

```yaml
status:
  phase: Discovered
  reason: GatewayAddressMissing
  message: "gateway address not yet available; cannot advance to Connecting"
  observedGeneration: 2
```

### Status conditions

The operator writes `Connected` and the roll-up beside the phase, which stays for
v1alpha1 readers. On a peer, `Connected` is the only gate. Under Local membership, a
`False` `SignalsReachable` or `CatalogAccepted` raises `Degraded`, as
[Peer conditions under Local membership](#peer-conditions-under-local-membership) lists.

A peer's `GridSite` is a local object, and its conditions record what this site
observes of that peer. Status is never shared between sites, so two sites can
disagree about one peer during a partition. The peer's own health is in its own
`GridSite` and its signals.

| Status | Reason | When |
|---|---|---|
| `True` | `Connected` | The TLS handshake verified the chain, identity and pin. |
| `Unknown` | `AwaitingFirstCheck` | An address is known and the first probe has not run. `Progressing` is `True` with `Probing`. |
| `False` | `AwaitingDiscovery` | The record waits for discovery to supply its address. |
| `False` | `AddressMissing` | No gateway address is configured. |
| `False` | `AddressRefused` | The gossiped address is not a dialable literal `IP:port`. |
| `False` | `Unreachable` | The TCP connect was refused or timed out. The message names which. After an address edit, `Progressing` is `True` with `Probing`. |
| `False` | `TLSProtocolError` | The TLS handshake failed or timed out at the protocol level. |
| `False` | `UntrustedIssuer`, `IdentityMismatch`, `CertificateExpired`, `CertificateNotYetValid`, `PinMismatch`, `TrustMaterialMissing`, `TrustMaterialInvalid`, `IdentityVerificationRequired` | The probe's trust failure, named as the probe reports it. |

`Connected` and the roll-up have separate writers. The phase stays a merge patch.

This cluster's own site rolls up the same way, from a probe of its own gateway, and
its GridSite says nothing about its identity. The identity conditions live on the
GridNetwork, where `Accepted`, `ResolvedRefs`, `Enrolled` and `IdentityValid` gate the
roll-up in that order. There `Degraded` is `True` while `RotationOnSchedule` is `False`,
the early warning that the identity is about to expire, and `Progressing` is `True`
while an enrollment is in flight or retried.

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

This external API example intentionally omits `healthCheck` because the API
does not expose a suitable `GET /health` endpoint. It also omits metrics
scraping because the external API does not provide the provider-pool metrics
used by AGN scoring. The referenced `openai-token` Secret must exist in
`praxis-system` before controller-managed
credential projection can become available.

To probe the provider's default `{spec.endpoint}/health` URL, add this to the
provider spec:

```yaml
healthCheck: {}
```

When `spec.auth` declares a controller-managed bearer token, the operator
automatically sends it on same-origin HTTPS health probes. If the endpoint
uses a private CA, also configure `healthCheck.tls.caSecretRef`:

```yaml
healthCheck:
  tls:
    caSecretRef:
      name: model-gateway-ca
      namespace: praxis-system
```

The token comes from `spec.auth.secretRef` on each probe. The probe target must
have the same scheme, host, and effective port as `spec.endpoint` to receive it.
`healthCheck.path` changes the request path; `healthCheck.endpoint` can change
the base URL within that origin. Probes to a different origin or over plain
HTTP remain anonymous. Providers without controller-managed bearer auth also
probe anonymously. Redirects are not followed; a 3xx response degrades health.

**Phases**: Pending → Available → Degraded → Unavailable

**Readiness**: `status.conditions` carries a `Ready` condition, written by the
operator's signals loop from each scrape of the provider's metrics, in every
signal mode. Readiness is
a condition, not a phase: `phase` follows the provider's configuration only, and
says nothing about whether it serves now.

| Status | Reason | When |
|---|---|---|
| `True` | `Ready` | The latest scrape succeeded with at least one ready endpoint. |
| `False` | `NoEndpointsReady` | Two consecutive scrapes counted zero ready endpoints, and the EPP recorded no engine answer in the last 30s. |
| `True` | `NoLivenessCheck` | The scrape answered without the pool's ready-endpoint series (`llm_d_epp_ready_endpoints`, then `inference_pool_ready_pods`, for `poolName`). Routing serves the provider, so `Ready` reads `True`: a provider pointed at vLLM's own `/metrics` carries no such series. The message names what was missing. |
| `False` | `ScrapeTimedOut` | No scrape succeeded within `staleMetricsSeconds` or the [readiness floor](polling-metrics.md#provider-readiness), if longer, and the latest timed out. |
| `False` | `ScrapeUnauthorized` | As above, and the latest was refused with 401 or 403. |
| `False` | `TLSHandshakeFailed` | As above, and the latest failed TLS, including the TLS material. |
| `True` | `MetricsBodyTooLarge` | The endpoint answered, but its metrics HTTP response body passed the operator's 1 MiB budget. The provider remains a routing candidate, so `Ready` reads `True`, and `MetricsReachable` reports the failure. |
| `False` | `ScrapeFailed` | As above, and the latest failed otherwise. The message names the class: `dns`, `connect`, `http`, `parse`, or `config`. |
| `False` | `MetricsStale` | No scrape succeeded within `staleMetricsSeconds` or the [readiness floor](polling-metrics.md#provider-readiness), if longer, and none failed. |
| `False` | `ProviderUnavailable` | The provider is `Unavailable`. |
| `Unknown` | `AwaitingFirstScrape` | No scrape has succeeded yet, within the grace window. |
| `Unknown` | `NotEvaluated` | No metrics endpoint is set, so readiness is not evaluated. `MetricsAvailable` reads the same. |

The operator logs each change of reason once: at WARN when the provider turns not
ready, at INFO when it returns to `Ready` or waits. Each scrape counts in
`grid_provider_scrape_total{grid_provider,result}`, where `result` is `success`,
`no_series`, or a failure class, and
`grid_provider_last_scrape_success_timestamp_seconds{grid_provider}` holds the time
of the last scrape with the ready-endpoint series.

A provider whose `Ready` is `False` is excluded from routing: its site publishes
`grid_provider_ready 0`, and its serving config entry carries `admission: none`.
See [Polling Cross-Site Load Signals](polling-metrics.md#provider-readiness).

**Roll-up**: `Available` is gated by `Accepted`, `ResolvedRefs` and
`EndpointLive`, in that order, and a `MetricsReachable` failure sets `Degraded`.
The provider reconciler writes `Accepted`, `ResolvedRefs` and the roll-up. The
signals loop writes `EndpointLive` and `MetricsReachable` with `Ready`.

| Condition | Status | Reason | When |
|---|---|---|---|
| `Accepted` | `False` | `Invalid` | The endpoint or a model name is blank, or `spec.auth` does not parse. |
| `ResolvedRefs` | `False` | `BackendNotFound` | The referenced `GridNetwork` does not exist. |
| `ResolvedRefs` | `False` | `NoSitesMatched` | Sites exist and `siteSelector` matches none. |
| `ResolvedRefs` | `Unknown` | `NotEvaluated` | An invalid spec or an unreadable credential stopped resolution first. |
| `EndpointLive` | `True` | `EndpointLive` | The latest scrape counted ready endpoints, or, without metrics, the health check passes. |
| `EndpointLive` | `True` | `NoHealthSignal` | Nothing checks liveness: no metrics endpoint and no health check, or metrics without a ready-endpoint series. Routing serves it, so the condition says so. |
| `EndpointLive` | `False` | `NoEndpointsReady` | Scrapes count zero ready endpoints, or the provider is `Unavailable`. |
| `EndpointLive` | `False` | `LivenessStale` | No scrape succeeded within the readiness window. `MetricsReachable` names why. |
| `EndpointLive` | `Unknown` | `AwaitingFirstCheck` | No scrape has succeeded yet, within the grace window. |
| `MetricsReachable` | `False` | The `Ready` scrape reason | The latest scrape failed. Absent when no metrics endpoint is set. |

`Programmed`, from model discovery, joins the gates later. The phase is computed
as before.


```text
NAME           PROVIDER      AVAILABLE   PROGRESSING   DEGRADED   SINCE   AGE
qwen3-site-a   self_hosted   True        False         False      3d      3d
qwen3-site-b   self_hosted   False       False         False      2m      3d
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
| `metricsConfig.endpoint` | absent | Optional metrics-service base URL. When set, it replaces `spec.endpoint` as the scrape base; `path` is appended to the selected base. |
| `path` | `/metrics` | HTTP path, relative to `metricsConfig.endpoint` when set, otherwise `spec.endpoint`. |
| `timeout` | `2s` | Scrape timeout. `s` and `ms` suffixes are recognized. |
| `poolName` | absent | Selects samples whose Prometheus `name` label matches this pool. When set, the scrape must contain at least one configured signal for that pool. |
| `queueCapacity` | absent | For raw queue-depth counts, divide by this positive capacity and clamp the normalized value to `0.0..1.0`. Without it, queue depth must already be normalized. |
| `signalNames` | all unset | Mapping from scoring signals to Prometheus metric names. |
| `staleMetricsSeconds` | absent | Maximum age in seconds for reusing the last successful sample after a failed scrape. Readiness uses this age or the [readiness floor](polling-metrics.md#provider-readiness), if longer. Minimum: `1`. For plaintext metrics, absence means immediate neutral fallback; when TLS is configured, an expired/absent sample makes the provider unhealthy and excluded. |
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
    endpoint: https://qwen3-epp-service.ai-tenant-site-a.svc:9090
    path: /metrics
    poolName: qwen3-inference-pool
    signalNames:
      queueDepth: llm_d_epp_average_queue_size
      kvCacheUtilization: llm_d_epp_average_kv_cache_utilization
    auth:
      type: serviceAccountToken
```

#### Queue depth normalization

AGN does not normalize raw queue counts. Exporters should publish `queueDepth`
as a normalized `0.0` to `1.0` gauge before the operator scrapes it.

### Model discovery

`spec.modelDiscovery` makes the operator poll the backend for the models it
serves. Discovered model names can appear in local route candidates only for a
gateway that declares `modelDiscoveryContractVersion: v1`. They remain
withheld from provider records because the current provider-record format
cannot carry expiry. A provider with discovery configured does not fall back to
`spec.models`; providers without discovery continue to use `spec.models`.

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

`status.servedModels` is the discovered set itself: the deduplicated model
names the loop last observed for this provider (the field is a set; order
carries no meaning). A successful poll writes the set and its expiry boundary,
including when the names are unchanged. An empty set is present and means the
backend serves no models.

`status.servedModelsExpiresAt` is the RFC 3339 UTC deadline for that exact set,
computed from the last successful poll and the discovery TTL. Readers must
require this timestamp to be in the future before using `servedModels`. The
discovery loop may not clear the status until its next round after the TTL, so
the presence of `servedModels` alone does not prove freshness. Failed polls do
not extend the deadline. When the set is unknown, expired and cleared, or
discovery is disabled, these discovery inventory fields are absent; readers
must fail closed.
`status.servedModelsObservedGeneration` records the provider `metadata.generation`
that produced the set. Readers must require it to equal the current generation;
a spec change makes the prior inventory unusable until a successful poll writes
a new set, expiry, and generation together.
Updating these fields advances `metadata.resourceVersion`, so the CRDT revision
of the gossiped provider record advances with discovery status changes.

### Gateway model discovery contract

The operator adds `expires_at` (RFC 3339 UTC) to each discovered local route
candidate. A gateway may receive these candidates only when its `GatewayRef`
declares `modelDiscoveryContractVersion: v1`, after the running image has
implemented this contract. At request time the gateway must treat the candidate
as expired once its UTC clock reaches `expires_at` minus 30 seconds. This
conservative allowance covers gateways whose clocks are up to 30 seconds behind
the operator; a gateway with a clock ahead may withdraw the route early. The
gateway must never extend the deadline or apply a grace period after it.
Gateways without the declaration receive no discovered candidates.

The embedded gateway carries the same deadline in its watched serving config as
`expires_at_ms`. It checks the deadline during route selection and pinned
requests, using the same 30-second allowance; expiry therefore remains enforced
even when operator reconciliation or config delivery stops.

The bearer token comes from `spec.auth`. A model-discovery URL must use HTTPS
when a bearer token is configured. With `auth.credentialsManagedExternally`, requests carry no
credentials, so plain HTTP remains available.

Discovery refuses loopback, `localhost`, link-local, unspecified, and cloud
metadata addresses, including the AWS, GCP, and EKS Pod Identity IPv6
endpoints. Other unique-local IPv6 addresses remain allowed. A literal blocked
IP or a `localhost` URL is reported as `config`; a hostname that resolves to a
blocked address is reported as `unreachable`.

Discovery runs in its own loop, like the signals scraper, not in reconcile.
Discovered sets are held in memory per provider:

- A successful poll replaces the set and renews its deadline; an empty list is
  a valid result.
- A failed poll keeps the set; its deadline is not renewed.
- A set not renewed within the TTL expires in the monotonic in-memory store.
  Readers use `servedModelsExpiresAt` to reject stale status even if the next
  discovery round has not cleared it yet.
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
partial-success state to represent: it either succeeds (`Available`) or
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
| `EndpointTlsSecretMissing` | Unavailable | A referenced TLS Secret does not exist (or the requested key is absent; see [`grid#58`](https://github.com/praxis-proxy/grid/issues/58) for a known misclassification of the latter). |
| `EndpointTlsKeyMissing` | Unavailable | The expected key exists in the Secret but its value is empty. |
| `EndpointTlsMaterialInvalid` | Unavailable | CA certificate PEM material could not be parsed. |
| `EndpointTlsIdentityMismatch` | Unavailable | Client certificate or private key PEM material could not be parsed. |

An empty `reason` with `phase: Available` means `SitesMatched`; an empty
`reason` with `phase: Pending` means `AwaitingSiteMatch`; both are
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
