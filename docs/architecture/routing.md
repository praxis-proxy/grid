# Routing

Grid routing is split between the Grid Operator control plane and the Praxis
data plane. The operator renders routing state. Praxis consumes that state and
proxies requests.

## Overview

```text
GridNetwork + InferenceProvider CRDs
  + provider metrics
  + CRDT provider records
        |
        v
Grid Operator
        |
        v
Versioned routing overlay ConfigMap
        |
        v
Praxis validates and accepts a routing snapshot
        |
        v
intelligent_route serves from that exact snapshot
        |
        v
Praxis provider gateway
        |
        v
llm-d / EPP / inference backend
```

Grid does not proxy traffic. It writes the overlay used by Praxis filters.

## Control-plane rendering path

For each `GridNetwork` and gateway reference, the operator:

1. Lists local `InferenceProvider` resources for the network.
2. Collects provider metrics from `spec.metricsConfig` when configured.
3. Reads remote provider records received through CRDT state.
4. Converts providers into scoring backends and routing candidates.
5. Scores and orders candidates.
6. Builds a versioned, content-addressed overlay envelope.
7. Server-side applies an overlay `ConfigMap`.
8. Reports rendered and distributed revision state on the `GridNetwork`.

The `ConfigMap` contains:

| Key | Purpose |
|---|---|
| `routing-overlay.json` | Versioned envelope with scope, provenance, digest, and routing payload. |
| `routing-config.json` | Bare routing payload for consumers that have not enabled the envelope contract. |

Both keys describe the same routing state.

## How re-ranking updates

Grid does not re-rank a provider inside the request path. Re-ranking happens
when the Grid operator reconciles a `GridNetwork` and publishes a new routing
overlay. Praxis then uses the most recent overlay that it has accepted.

The complete update path is:

```text
provider metrics / Kubernetes state / remote Grid state changes
                              |
                              v
                    Grid operator reconcile
                              |
              scrape and normalize provider metrics
                              |
                  score and order candidates
                              |
          write a new versioned overlay ConfigMap
                              |
                 ConfigMap watch / projection
                              |
                    overlay-sync, if enabled
                              |
                    Praxis atomic hot reload
                              |
              next request uses the new ordering
```

### What causes a reconcile

The operator has two kinds of triggers:

| Trigger | What happens | Typical timing |
|---|---|---:|
| `InferenceProvider` change | Provider health, endpoint, model, metrics, or configuration changes can enqueue the owning `GridNetwork`. | Immediate watch event |
| `GridSite` change | Site labels, geography, membership, or site status changes can enqueue the owning `GridNetwork`. | Immediate watch event |
| `GridNetwork` change | A change to the network or gateway references enqueues that network. | Immediate watch event |
| Remote SWIM/CRDT state observed | Remote provider and site state is consumed during reconciliation. | On the next reconcile/event-driven enqueue |
| Periodic requeue | The operator periodically re-scrapes metrics and re-renders overlays. | **300 seconds by default** |
| TLS metrics configuration | A network with any TLS-protected metrics provider uses a shorter bounded requeue so certificate rotation is noticed without a Secret watch. | **60 seconds** |

The periodic interval is important for live metrics: a changing EPP queue does
not itself change a Kubernetes resource, so it normally waits for the next
`GridNetwork` reconcile. A provider or site watch can cause an earlier
reconcile for other state changes.

Set `spec.metricsRefreshInterval` when a deployment needs a different periodic
cadence:

```yaml
spec:
  metricsRefreshInterval: "10s"
```

The value accepts seconds or millisecond durations of at least one second; the
CRD rejects zero, subsecond, and unsupported formats before they reach the
operator. The controller also fails reconciliation if malformed data reaches
it outside the Kubernetes admission path. An absent value uses the safe
default of 300 seconds for plaintext metrics. A network with TLS
metric credentials defaults to 60 seconds and never permits a custom interval
longer than that bound, so certificate rotation detection is not delayed.
Shorter intervals increase Kubernetes API, metrics-scrape, and overlay-update
load. Ten seconds is a reasonable demo value: it is responsive enough to show
EPP pressure transitions while avoiding the extra churn of a five-second loop.
Use shorter intervals only for deliberately latency-sensitive deployments.

### What happens during re-ranking

During reconciliation, Grid:

1. Lists the eligible `InferenceProvider` resources and `GridSite` resources.
2. Scrapes each configured metrics endpoint, such as an llm-d EPP endpoint.
3. Normalizes the signal selected by `scoringPolicy` and applies freshness and
   health handling.
4. Builds the candidate set, including eligible remote providers received
   through Grid state.
5. Computes the selected strategy's score and applies admission and
   `routingPolicy` ordering. The first candidate receives rank `0`, the next
   receives rank `1`, and so on.
6. Renders a content-addressed overlay for each gateway reference. Each
   gateway can therefore receive a different ranking because its `localSite`
   and routing perspective can differ.
7. Applies the overlay ConfigMap and records rendered/distributed revision
   status.

If rendering or applying a new overlay fails, Grid retains the previously
distributed revision rather than publishing a partial routing state.

### How the new overlay reaches Praxis

After Grid applies the ConfigMap, delivery is separate from re-ranking:

- With `overlay-sync` enabled, the sidecar watches the named ConfigMap,
  validates the envelope and digest, atomically replaces the serving file, and
  exposes readiness/liveness state.
- Without `overlay-sync`, Praxis uses the direct ConfigMap mount and its normal
  file-watch/projection behavior.
- Praxis validates the new overlay and atomically swaps its in-memory snapshot.
  Invalid updates leave the last-known-good snapshot serving; no request reads
  Kubernetes or Grid directly.

Overlay-sync can remove kubelet projection delay, but it does **not** make
metrics collection or operator reconciliation more frequent. The end-to-end
route-change time is the sum of metric availability, the next reconcile,
ConfigMap application, overlay delivery, and Praxis hot reload.

### Demo-specific forced refresh

The LLM-D pool metrics demo deliberately annotates the `GridNetwork` after a
pressure or recovery transition. That annotation creates an immediate watch
event and bypasses the normal 300-second periodic requeue, allowing the demo
to show the queue change, re-ranking, overlay revision, and routed request in
one controlled flow. This is orchestration behavior for the demo; it is not a
different Grid scoring algorithm.

## Routing overlay format

The envelope is the authoritative observable contract consumed by Praxis AI:

```json
{
  "schema_version": "1.0.0",
  "revision": {
    "kind": "content_addressed",
    "algorithm": "sha256",
    "value": "64-lowercase-hex-characters"
  },
  "content_digest": {
    "algorithm": "sha256",
    "value": "64-lowercase-hex-characters"
  },
  "scope": {
    "network": "production",
    "gateway": "inference-edge",
    "namespace": "praxis-system",
    "local_site": "site-east"
  },
  "provenance": {
    "producer": "grid-operator",
    "producer_version": "0.1.0",
    "source_name": "production",
    "source_uid": "00000000-0000-0000-0000-000000000000",
    "source_generation": 12,
    "rendered_at": "2026-07-29T00:00:00Z"
  },
  "overlay": {
    "network": "production",
    "local_site": "site-east",
    "candidates": []
  }
}
```

The nested `overlay` is the compact routing payload:

```json
{
  "network": "production",
  "local_site": "site-east",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "model-east",
      "site": "site-east",
      "cluster": "gateway-site-east",
      "fresh": true
    },
    {
      "kind": "inference_model",
      "name": "model-west",
      "site": "site-west",
      "cluster": "gateway-site-west",
      "fresh": true,
      "credential": {
        "strategy": "bearer_token",
        "secretRef": {
          "name": "west-api-token",
          "namespace": "grid-system",
          "key": "token"
        }
      }
    }
  ]
}
```

### Revision semantics

The v1 revision is the lowercase SHA-256 digest of the RFC 8785 canonical form
of these routing-relevant fields:

- `network`
- `local_site`
- the ordered `candidates` array

Timestamps, provenance, Kubernetes metadata, and envelope annotations are not
part of the semantic payload. Re-rendering identical routing state therefore
produces the same revision. Candidate membership, order, admission, locality,
freshness, credential references, or other serialized candidate content
changes the revision.

In schema v1, `revision.value` and `content_digest.value` are identical. Praxis
AI rejects an envelope when either value is malformed, the values disagree, or
the recomputed digest does not match.

### Scope and provenance

Scope binds an overlay to one network, gateway, namespace, and local site.
Praxis AI deployments configure the scope they expect and reject a validly
encoded overlay intended for a different gateway. Enabling expected scope also
requires the envelope format; the consumer cannot silently downgrade to the
unscoped legacy payload.

Provenance identifies the producing Grid operator and source `GridNetwork`.
Praxis AI validates required values and bounds before accepting the envelope.
Provenance supports audit and diagnosis but is not an authorization credential.

The `ConfigMap` repeats the schema version, semantic revision, and content
digest in `grid.praxis-proxy.io/*` annotations so Kubernetes tooling can inspect
the contract without decoding the data value.

### Revision lifecycle

The contract distinguishes four observable stages:

| Stage | Owner | Evidence |
|---|---|---|
| **Rendered** | Grid operator | `GridNetwork.status.overlayStatus[].renderedRevision` |
| **Distributed** | Grid operator and Kubernetes | `distributedRevision` plus the applied `ConfigMap` `resourceVersion` |
| **Accepted** | Praxis AI | Successful validation and atomic snapshot-load event |
| **Serving** | Praxis AI request path | The selected immutable snapshot revision attached to provider-hop telemetry |

On a successful apply, rendered and distributed revisions match. If Grid
renders revision B but cannot apply it, status can report rendered B while
retaining distributed A and its `resourceVersion`. This distinction prevents a
render attempt from being mistaken for a deployed routing change.

Praxis AI performs bounded reads and strict validation before replacing its
in-memory snapshot. Invalid cold-start state fails closed. An invalid update
retains the same-process last-known-good snapshot, and an unchanged semantic
revision does not cause a replacement. The request path reads the immutable
accepted snapshot from memory and does not read Kubernetes, the filesystem,
SWIM, or Grid APIs.

For a provider hop, `intelligent_route` removes any caller-supplied revision context
and sets `x-ai-routing-revision` from the exact snapshot that served the
selection. The provider gateway validates its syntax, consumes it after peer
authentication, removes it from the forwarded request, and sets
`x-ai-provider-routing-revision` for backend telemetry. These headers provide
correlation; they do not replace mTLS identity or provider-local route policy.

Candidate fields:

| Field | Meaning |
|-------|---------|
| `kind` | Capability kind, currently `inference_model` for model routing. |
| `name` | Model or capability name matched by the consumer gateway. |
| `site` | Grid site advertising the capability. |
| `cluster` | Praxis load-balancer cluster identity used for upstream routing. |
| `fresh` | Whether provider status is considered fresh enough for normal routing. |
| `credential` | Optional. Secret reference for upstream authentication. Present only for `api_provider` or authenticated `cloud_managed` candidates. **Never contains the token value** — only the Kubernetes Secret locating information. |
| `stable_id` | Optional. Deterministic FNV-1a hash of `{kind}/{name}/{site}/{cluster}`. Used as `candidate_id` in provider gateway `provider_route` configuration. This is distinct from the InferenceProvider CR `.metadata.name`. Also suitable for consumer-side session binding keys. |
| `admission_state` | Optional. Bounded admission state: `"new_and_existing"`, `"existing_only"`, or `"none"` (excluded). Derived from provider health and capacity metrics. |
| `selection_tier` | Optional. Locality tier between consumer gateway and provider: `"same_site"`, `"same_zone"`, `"same_region"`, `"cross_region"`, or `"unknown"`. Derived from `GridSite` region and zone. |
| `rank` | Optional. Zero-based position in the final sorted overlay. |

Overlay-level fields:

| Field | Meaning |
|-------|---------|
| `generated_at` | Optional. RFC 3339 timestamp of when the overlay was rendered. |

The metadata fields above belong to the generic Praxis AI routing contract.
Grid supplies Grid-specific values through that contract. The generated Praxis
static `intelligent_route` configuration intentionally strips
`stable_id`, `admission_state`, `selection_tier`, `rank`, and `generated_at`
because current static `intelligent_route` candidate config rejects unknown fields.
Praxis overlay-file hot reload consumes the producer's raw overlay and must remain
forward-compatible with unknown overlay metadata fields.

### Credential field security contract

When `credential` is present on a candidate, the field contains only:
- `strategy`: the authentication mechanism (currently `"bearer_token"`)
- `secretRef.name` / `secretRef.namespace` / `secretRef.key`: Kubernetes Secret locating information

The token value is **never written into the overlay ConfigMap**. The `intelligent_route`
filter parses the field and makes it available to downstream filters, but does
not perform Kubernetes API calls or inject credentials itself.

Credential injection is handled by the final-hop gateway that makes the final
backend call.  For direct API-provider or cloud-provider fallback, the consumer
gateway is often also the final-hop gateway, so it mounts the Secret and Praxis
AI injects the credential before forwarding to the provider API.  For remote
Grid sites, provider backend credentials stay in the remote provider site or
provider-side component; the consumer gateway should not receive those provider
tokens.

Native file-backed injection requires the Praxis AI `credential_inject`
filter.  Grid can render the overlay and generated config for that path today,
but runtime deployments must use a Praxis AI image that includes the filter.

## Candidate scoring and ordering

The operator orders candidates before writing the overlay. Ordering proceeds
in two phases:

1. **Scoring.** Each provider is scored by `scoring::score_backends` using
   the weights selected by `GridNetwork.spec.scoringPolicy.strategy`,
   optional live metrics, and optional CRDT-propagated provider metrics.
   Providers with no live metrics use neutral metric scores.
   See [Scoring](scoring.md) for the available strategies.

2. **Enrichment and ordering.** Each candidate is enriched with admission
   state, locality tier, stable ID, score, score breakdown, and rank.
   Candidates whose admission state is `"none"` (unhealthy) are removed.
   `GridNetwork.spec.routingPolicy` then selects one of two orderings:

   | Policy | Order |
   |---|---|
   | `geographyFirst` (default) | admission, locality, score descending, freshness, deterministic tie-break |
   | `scoreFirst` | admission, freshness, score descending, locality, deterministic tie-break |

   The deterministic tie-break is `(site, name, cluster)`. After
   deduplication, each candidate receives a zero-based `rank`.

Both policies ensure that a healthy cross-region provider
(`new_and_existing`) ranks before an overloaded same-region provider
(`existing_only`). Admission state takes priority over locality and score.
`scoreFirst` additionally allows comparable runtime metrics to move a remote
provider ahead of a local provider before either reaches an admission
threshold.

The enrichment and ordering phase runs after existing hard constraints (auth,
capability, access policy, freshness, phase filters). It does not override
data-residency or sovereignty constraints; those must be enforced separately
before candidates reach the ordering phase.

### Admission state derivation

When `spec.admissionPolicy` is omitted, admission preserves the legacy
instantaneous behavior:

| Condition | State |
|-----------|-------|
| No metrics available | `new_and_existing` |
| `healthy = false` | `none` (excluded from overlay) |
| `queue_depth > 0.85` or `kv_cache_utilization > 0.90` | `existing_only` |
| Otherwise | `new_and_existing` |

New installations from the `grid-site` Helm chart explicitly select the
stabilized policy. Stabilized admission requires repeated pressure observations
before entering `existing_only`, and repeated low-pressure observations plus a
minimum state duration and recovery hold-down before returning to
`new_and_existing`. Missing or expired metrics fail closed to `existing_only`
by default (or `none` when `missingMetrics: excluded`). Hard health failures
still move immediately to `none`. The evaluator is control-plane state keyed by
provider identity; no admission check runs in the request path.

The wire states remain `new_and_existing`, `existing_only`, and `none`, so this
change is compatible with existing Praxis consumers. Restarting the operator
resets the hysteresis counters and requires fresh observations before a
restrictive provider is promoted.

### Locality tier derivation

Locality tier is derived from `GridSite.spec.region` and `GridSite.spec.zone`:

| Condition | Tier |
|-----------|------|
| Consumer and provider are the same named site | `same_site` |
| Same region **and** same zone | `same_zone` |
| Same region, different zone | `same_region` |
| Different regions | `cross_region` |
| Either site has no region | `unknown` |

Zone comparison requires a region match because zone names are not globally
unique.  When no `GridSite` geography is configured, all candidates receive
`unknown` tier and ordering falls through to score-based ranking — preserving
backward compatibility with deployments that predate geography fields.

`Unavailable` providers are excluded. `Degraded` providers remain in the
overlay with `fresh: false`. Providers with no live metrics use neutral metric
scores.

At request time, `intelligent_route` selects from this pre-sorted candidate list rather
than recomputing the full scoring formula.

## Stale candidate retention and expiry

### Policy

Stale candidates (`fresh: false`) are **retained in the overlay** rather than
immediately excluded.  This policy supports:
- **Observability** — operators can see that a remote peer is degraded before
  it recovers.
- **Last-resort fallback** — if no healthy candidate exists for a model, a
  stale candidate is better than a hard 404.

The authoritative GC policy function is `should_retain_candidate` in
`operator/src/resources/routing_overlay.rs`.  Rules, in priority order:

| Condition | Result |
|---|---|
| `fresh = true` | Always retain (local and healthy remote candidates) |
| No TTL configured | Retain indefinitely (current default) |
| Age unknown | Retain conservatively |
| Age < TTL | Retain (within the allowed window) |
| Age ≥ TTL | Evict |

### SWIM member age

`MemberRecord.age_secs` tracks the elapsed time since a member last transitioned
to `Dead` or `Suspect`.

The SWIM runtime (`operator/src/swim_runtime.rs`) records the transition instant
in a private `status_changed_at: Option<Instant>` field for each member.  When a
member transitions to `Dead` or `Suspect`, the instant is recorded and preserved
monotonically.  When the member rejoins (`Alive`), the instant is cleared.  The
public `MemberRecord.age_secs` is computed as `now.saturating_duration_since(status_changed_at).as_secs()`
at snapshot time.

A `age_secs = 0` has two distinct meanings:
- **Alive member** — no Dead/Suspect transition has occurred.
- **Dead/Suspect member with `age_secs = 0`** — the runtime has just transitioned
  (elapsed is less than one second), or a synthetic snapshot did not include age.
  The GC helper `dead_or_suspect_age_secs` treats `age_secs = 0` on a
  Dead/Suspect member as "unknown" and retains conservatively.

**`crdt::ProviderState`** carries only a monotonic `revision` counter, not
a wall-clock timestamp.  CRDT storage-level GC is outside the current operator contract.

### Per-GridNetwork TTL — `spec.staleCandidateTtlSeconds`

The `GridNetwork` CRD exposes `spec.staleCandidateTtlSeconds` (optional `u32`)
to control when stale candidates are removed from the overlay.

| `spec.staleCandidateTtlSeconds` | Behaviour |
|---|---|
| Absent (default) | No-op — stale candidates retained indefinitely |
| `0` | Rejected by the CRD schema (`minimum: 1`) |
| `N >= 1` | Remote `fresh=false` candidates with SWIM member age `>= N` seconds are omitted from the overlay |

The filter runs every reconcile cycle after `apply_swim_staleness_override`.
Only remote candidates in the `Degraded` phase are subject to GC.  Local
candidates and `Available` remote candidates are always retained.

The controller also defensively treats an internally observed `0` as absent, so
malformed data cannot accidentally trigger immediate eviction outside the normal
Kubernetes API validation path.

**Recommended starting value:** `3600` (one hour) — allows short outages to
recover without overlay churn while bounding accumulation of truly dead peers.

**Important:** The TTL is applied at overlay-rendering time.  CRDT provider
records in storage are not deleted by this mechanism.  CRDT storage-level GC
is outside the current operator contract.

### Not implemented: hard exclusion

The GC policy does not implement hard exclusion of all `fresh=false` candidates.
A `fresh=false` candidate is only evicted after the TTL expires; it is
**deprioritized**, not excluded.  See the scoring section for how `fresh=false`
affects candidate ordering.

## Backend kinds

`InferenceProvider.spec.backendKind` is a placement and policy category. It is
not strictly a wire-protocol choice, and it does not by itself mean a route does
or does not use a Praxis gateway.

| `backendKind` | Meaning | Typical path | Placement intent |
|----------------|---------|--------------|------------------|
| `local` | Self-hosted capacity in the local site. | Consumer Praxis directly to local/provider-side Praxis or local backend cluster. | Prefer first when healthy. |
| `remote` | Self-hosted capacity in another Grid site. | Gateway-to-gateway mTLS to a remote Praxis provider gateway. | Prefer after local Grid-owned capacity. |
| `cloud_managed` | Managed model capacity under the operator's cloud account. | Praxis gateway, provider adapter, or direct managed-service endpoint depending on deployment. | Prefer after Grid-owned capacity and before generic SaaS fallback. |
| `api_provider` | Third-party API/SaaS provider fallback. | Praxis injects configured provider credential and forwards to the API endpoint. | Last-resort or explicit fallback tier. |

`cloud_managed` is distinct because Grid should apply different cost,
credential, observability, and placement assumptions than it applies to
self-hosted sites. A deployment may still place Praxis in front of a
cloud-managed backend; the category describes operational ownership, not a
requirement to bypass Praxis.

## Multi-cluster model routing

Multi-cluster model routing is the baseline Grid data-plane behavior:

1. Each provider site declares the models it can serve through
   `InferenceProvider.spec.models`.
2. `spec.routingClusterRef` names the Praxis upstream cluster that can reach
   that provider site.
3. The operator renders one overlay candidate per routable model/provider pair.
4. The consumer Praxis gateway extracts the requested model and selects the
   first matching candidate from the ordered overlay.
5. For remote sites, traffic goes gateway-to-gateway over mTLS before reaching
   provider-local filters and serving infrastructure.

Example overlay shape:

```json
{
  "kind": "inference_model",
  "name": "model-west",
  "site": "site-west",
  "cluster": "gateway-site-west",
  "fresh": true
}
```

In that example, a request for `model-west` selects the `gateway-site-west`
Praxis cluster. The concrete pod or endpoint inside `site-west` is still chosen
by the provider-side serving stack, such as llm-d/EPP endpoint selection.

## API-provider fallback

API-provider fallback uses the same overlay mechanism as self-hosted routing.
The difference is the backend category and credential handling:

1. An `InferenceProvider` declares `backendKind: api_provider`.
2. The operator includes the API provider as a candidate when it is available.
3. Scoring normally places self-hosted candidates ahead of API-provider
   candidates, so API providers are used as fallback or explicit lower-priority
   routes.
4. Praxis AI applies credential injection before forwarding the request to the
   provider endpoint (see "Credential injection" below).
5. If no self-hosted candidate is available for a model, the API-provider
   candidate can become the selected route.

The fallback decision is therefore still local to the consumer gateway at
request time: `intelligent_route` selects from the pre-rendered candidate list, and the
Praxis AI filter chain handles credential injection and upstream forwarding.

Grid overlay and credential-injection validation is distinct from
provider-protocol acceptance. Each external provider integration must
separately qualify its protocol, authentication exchange, timeout and retry
semantics, error mapping, streaming behavior, and credential rotation.

## Credential injection

When an `InferenceProvider` has `spec.auth.strategy: bearer_token` with a
`spec.auth.secretRef`, the operator projects a credential reference — never the
token value — into the routing overlay candidate:

```json
{
  "kind": "inference_model",
  "name": "model-z",
  "site": "api-provider",
  "cluster": "gateway-api-provider",
  "fresh": true,
  "credential": {
    "strategy": "bearer_token",
    "secretRef": {
      "name": "my-api-token",
      "namespace": "grid-system",
      "key": "token"
    }
  }
}
```

### Native injection path (current)

The native injection path uses two gateway filters in sequence:

1. **`intelligent_route`** selects the candidate and writes the secretRef fields to
   in-process filter metadata: `intelligent_route.credential.strategy`,
   `intelligent_route.credential.name`, `intelligent_route.credential.namespace`,
   `intelligent_route.credential.key`.  No token value is written.

2. **`credential_inject`** reads those metadata keys, looks up the
   matching token in its configured credential map, and injects
   `Authorization: Bearer <token>` into the upstream request.

Consumer config filter chain ordering:

```text
intelligent_route              → selects candidate; writes credential metadata
credential_inject  → reads credential metadata; injects Authorization
load_balancer           → upstream cluster selection with injected headers
```

This filter chain requires a Praxis AI image that includes
`credential_inject`.  Grid renders the overlay and generated config shape;
the runtime image must provide the filter implementation.

### File-backed token source

In the current xtask validation mode for direct API-provider fallback, the token
value is resolved from a Kubernetes Secret by the xtask harness and written into
a Kubernetes Secret in the consumer cluster.  The consumer pod mounts that
Secret as a file, and `credential_inject` reads the token from its
configured `file:` path at filter construction time.

In production, the same rule applies at the final-hop point: mount the Secret
only into the final-hop gateway or provider-side component that makes the final
backend call. Grid does not copy Secret values across clusters.

The token does NOT appear in:

- The Grid operator overlay `ConfigMap` (JSON).
- The `intelligent_route` filter candidates YAML.
- The consumer Praxis `ConfigMap`.
- The `intelligent_route.*` in-process filter metadata.
- Tracing spans or log lines.
- HTTP error response bodies.

### Deployment ownership

The operator generates the consumer Praxis config including the `credential_inject`
section for direct API-provider routes.  Secret provisioning — creating,
rotating, and synchronizing the mounted credential Secret in the final-hop
cluster — is the responsibility of platform automation or an external Secret
manager.

The `intelligent_route` → `credential_inject` filter chain interface is the same
regardless of how the final-hop Secret is provisioned.

## Routing eligibility

The Grid operator enforces a routing eligibility gate on remote provider state
received over SWIM CRDT broadcasts.  A remote provider record is included in the
routing overlay only when the corresponding `GridSite.status.phase` is `Active`.

| Site state | Remote CRDT providers eligible |
|---|---|
| No matching `GridSite` | No — fail-closed |
| `Pending` | No |
| `Discovered` | No |
| `Connecting` | No |
| `Active` | Yes — control-plane eligible |
| `Unreachable` | No |
| `Left` | No |

The matching rule: for a remote CRDT provider with `site_id = S` in network `N`,
the operator looks for a `GridSite` resource whose Kubernetes name equals
`discovered_site_k8s_name(N, S)` (the auto-discovered name derivation) and whose
`spec.gridNetworkRef == N` and `status.phase == Active`.

`Active` indicates control-plane eligibility: the operator has verified the remote
site's certificate fingerprint against the configured trust policy and confirmed TCP
connectivity to the gateway. This allows the site's providers to appear in routing
overlays for consideration by consumer gateways.

GridSite Active is a control-plane eligibility signal. It means Grid has enough
site/trust information to consider the site for overlay generation. It does not
currently prove that a Praxis gateway has completed an mTLS handshake, accepted
client identity, loaded the latest routing config, or authorized provider-side
traffic. Data-plane readiness is enforced separately at request time.

See [Authentication and Access Policy](auth.md) for the trust contract.

**Local providers** (from `InferenceProvider` resources in the same cluster) are
always eligible.  They are not filtered by `GridSite.status.phase`.

**Claim**: SWIM membership + TCP reachability + public cert material alone are not
sufficient for a remote provider to become routable.  `Active` is the explicit
routing eligibility gate; the operator only sets it after the configured
fingerprint trust policy matches.

**Validation**: `verify-swim-mesh-three-node` proves the eligibility gate in a
three-node mesh (A→B→C topology).  It asserts that C's provider is absent from
A's overlay before C's `GridSite` is `Active`, and appears only after `Active`
is set — even though CRDT state from C reached A transitively through B.  The
same validation confirms wrong-network provider records are absent from A's
correct-network overlay.

## Consumer gateway selection

At request time, `intelligent_route` matches the requested model against the
already-loaded overlay candidates, then chooses from Grid's pre-rendered
candidate order.  It does not call Kubernetes, SWIM, or the operator, and it
does not recompute the full scoring formula per request.

The Praxis consumer gateway extracts request facts such as the requested model
and runs `intelligent_route` against the overlay. For model inference, the filter scans
for matching `inference_model` candidates and sets the selected Praxis upstream
cluster.

If no candidate matches, the request fails cleanly instead of falling through to
an unrelated backend.

### External edge routing

External client ingress uses the same overlay consumption path. A Praxis AI
edge-ingress gateway loads a Grid overlay rendered for that edge's routing
perspective and runs `intelligent_route` identically to a cluster-local consumer
gateway.

The difference is upstream of `intelligent_route`: the external edge sits behind a
global traffic manager that selects a healthy edge before the request body or
model is known.  The edge then parses the OpenAI-compatible request, extracts
the model, and runs the loaded overlay selection.  Two-stage routing separates
edge selection (network proximity, health) from provider selection (model,
policy, capacity, location affinity).

Grid renders a per-gateway overlay specific to each edge's site and region.
With overlay-file hot reload enabled, the edge validates a replacement overlay
and swaps its in-memory snapshot atomically without process restart. Each
request uses one loaded snapshot for selection.

See [External Client Ingress](external-ingress.md) for the full external
routing design.

## Provider gateway trust

Provider gateways terminate mTLS before forwarding traffic to local inference
infrastructure. When the gateway image includes the `peer_identity_trust` filter,
provider gateways verify the peer identity from the downstream client certificate
and reject untrusted peers with HTTP 403 before forwarding to local infrastructure.

Provider gateways require a client certificate and can match both its exact
digest and organization. Certificate identity authorizes the peer; it does not
authorize an arbitrary candidate, model, or backend.

## Provider-side request forwarding

After site selection, the edge AI filter removes inbound copies of the fixed
`X-AI-Routing-*` contract and reconstructs the selected stable candidate and hop
request ID. The provider consumes those fields only after mTLS and
`peer_identity_trust`. `provider_route` removes them, validates an exact
provider-local candidate/model/path map, and selects the local backend cluster.
It performs no discovery, scoring, affinity, or hot reload.

Praxis AI rejects provider pipelines at startup unless the listener requires
client certificates and an unconditional, fail-closed `peer_identity_trust` is
the first filter before the top-level `provider_route`. First position
prevents an earlier branch from bypassing peer authorization. The validation
cannot be downgraded with the generic pipeline-validation skip options.

The provider inference path is:

```text
intelligent_route
  -> mTLS provider hop
  -> peer_identity_trust
  -> inference parser
  -> provider_route
  -> optional credential_inject
  -> load_balancer
  -> private backend
```

A provider can then use a local llm-d-style scheduling path:

```text
provider gateway
  -> ext_proc
  -> llm-d external processor
  -> endpoint_selector
  -> inference pod or service
```

Grid chooses the provider site. llm-d or the provider-local scheduler chooses
the concrete pod, GPU, or endpoint inside that site. Envoy ExternalProcessor
service integration is owned by
[`praxis-proxy/extproc`](https://github.com/praxis-proxy/extproc); it is an
optional provider-local integration and is not part of the Grid overlay
contract.

## Metrics and CRDT inputs

Local provider metrics enter routing through `InferenceProvider.spec.metricsConfig`.
Remote provider records enter routing through CRDT state broadcast over SWIM.
Both inputs are converted into the same scoring model before overlay rendering.

Remote records are filtered by network and local site identity so a site does
not route to its own CRDT echo or to providers from another `GridNetwork`.

## Metrics normalization contract

The `scoring::BackendMetrics` struct is the handoff point between metrics
ingestion and the scoring engine.  The following table defines the normalization
responsibility at each layer:

| Signal | Expected range in `BackendMetrics` | Normalization owner |
|---|---|---|
| `error_rate` | `[0.0, 1.0]` (ratio) | Prometheus exporter; clamped in the operator ingestion layer |
| `healthy` | `bool` | Derived by the operator from a health gauge or error rate |
| `kv_cache_utilization` | `[0.0, 1.0]` (ratio) | Prometheus exporter; clamped in the operator ingestion layer |
| `latency_p99_ms` | `≥ 0.0 ms` (raw milliseconds) | Prometheus exporter exposes a pre-computed P99 gauge; the **scoring engine** normalizes internally using `MAX_LATENCY = 5000 ms` |
| `prefix_cache_hit_ratio` | `[0.0, 1.0]` (ratio) | Prometheus exporter; clamped in the scoring engine |
| `queue_depth` | `[0.0, 1.0]` (ratio) | Exporter or recording rule; alternatively Grid divides a raw count by `metricsConfig.queueCapacity` |

### Destination-normalized metrics preferred

Sites and clusters should normalize their own capability metrics where
possible: the Prometheus exporter (or a recording rule on the destination)
converts raw queue depths to a `[0.0, 1.0]` ratio. This remains the preferred
pattern because heterogeneous sites can adapt normalization to their local
context. When an llm-d EPP exposes an absolute average queue size, set
`metricsConfig.queueCapacity`; Grid divides the raw value by that capacity and
clamps the result to `[0.0, 1.0]`.

For cloud-managed providers and third-party APIs where the destination
cannot export normalized metrics, the Grid operator may apply an adapter
when the normalization contract is stable.

### Missing-value defaults

When a provider has no live metrics (no `spec.metricsConfig`, scrape
failure, or absent CRDT record), scored signals default to neutral values
and health/error signals default to no evidence of failure:

| Signal | Default | Effect |
|---|---|---|
| `error_rate` | `0.0` | No evidence of errors; used for health derivation, not direct scoring |
| `healthy` | `true` | Assume reachable until evidence of failure |
| `kv_cache_utilization` | `0.5` | Neutral |
| `latency_p99_ms` | `2500.0 ms` | `1.0 - 2500/5000 = 0.5` neutral latency score |
| `prefix_cache_hit_ratio` | `0.5` | Neutral |
| `queue_depth` | `0.5` | Neutral |

### NaN and infinity

Prometheus scraping drops NaN and ±Inf values at parse time.  CRDT values
are treated as absent when non-finite and then defaulted/clamped in
`crdt_metrics_to_backend`.  The scoring engine does not re-validate for
NaN/Inf; callers must not propagate non-finite values.

### Stale metrics grace period

By default, a Prometheus scrape failure immediately causes the provider to
fall back to neutral (0.5) scoring for all signals.  When
`spec.metricsConfig.staleMetricsSeconds` is set, the operator keeps a
cross-reconcile cache of the last successful scrape for each provider.  If
the current scrape fails but the cached sample is no older than
`staleMetricsSeconds`, the cached values are used instead of neutral
scoring.

After the grace period expires the provider reverts to neutral scoring.
The cache is per-operator-process; restarting the operator clears all
cached samples.

`staleMetricsSeconds` has no effect on successful scrapes — fresh scraped
values always win.  Setting it only extends the window in which a
temporarily-unavailable endpoint's last known metrics influence scoring.

The field is optional.  When absent (default), the behaviour is unchanged
from before it was added: scrape failures produce neutral scoring
immediately.

### KV-cache affinity

Routing decisions based on KV-cache affinity — routing requests to backends
that already hold relevant KV-cache entries — are not implemented in the current
operator.  The `kv_cache_utilization` signal influences scoring but does not
implement affinity-aware routing.

## When the routing overlay regenerates

The overlay `ConfigMap` is regenerated by the Grid Operator whenever the owning
`GridNetwork` reconciles.

| Trigger | Effect |
|---|---|
| `GridNetwork` created or updated | Immediate reconcile; overlays regenerated |
| `InferenceProvider` created, updated, or deleted | Owning `GridNetwork` reconcile triggered; overlays regenerated |
| `GridSite` created, updated, or deleted | Owning `GridNetwork` reconcile triggered; overlays regenerated |
| Periodic requeue | Every 300 seconds by default; overlays regenerated |

During each render pass, the operator uses the current local CRDs, current
provider metrics, and current SWIM/CRDT state it has received so far.

Overlay regeneration is reconcile-driven, not per-request.  If a remote cluster
disappears, the overlay is not rewritten at packet time — it updates when the
operator's next reconciliation loop observes the new SWIM/member/provider state
and re-renders.

Rendering or distributing a new `ConfigMap` does not mean the gateway accepted
it. The recommended deployment uses `grid-overlay-sync` to watch the named
`ConfigMap` through the Kubernetes API, validate each content-addressed
envelope, and atomically write accepted revisions into a shared `emptyDir`.
Praxis watches that file and installs the new snapshot without a pod restart.

This avoids depending on kubelet's eventual projected-ConfigMap refresh loop,
which can leave the gateway serving an older route after the operator has
published a new preference. It also adds validation, atomic writes,
last-known-good retention, delivery health, and revision metrics at the handoff
boundary. See [Architecture Overview](overview.md#configmap-handoff) and the
[Praxis gateway chart](../../charts/praxis-gateway/README.md#routing-overlay-delivery).

Direct ConfigMap projection remains available when
`overlay.sidecar.enabled=false`, but its delivery latency is controlled by the
kubelet and it does not provide the sidecar's validation or delivery status.

Consumers that do not enable overlay-file reload still require a rollout or
another deployment-owned reload mechanism. See
[Consumer Config](consumer-config.md#reload-and-rollout).

## Relevant files

| File | Role |
|------|------|
| `operator/src/controller/grid_network.rs` | Reconcile loop wiring for metrics, CRDT snapshots, overlay rendering, and status. |
| `operator/src/resources/routing_overlay.rs` | Provider-to-candidate mapping, scoring input construction, and overlay JSON rendering. |
| `operator/src/resources/overlay_envelope.rs` | Envelope construction, RFC 8785 canonicalization, semantic digest, scope, and provenance. |
| `operator/src/resources/provider_metrics.rs` | Prometheus scrape and metric-name mapping for `metricsConfig`. |
| `scoring/src/scoring.rs` | Backend scoring engine (strategy-selected signals). |
| `swim/src/state_broadcast.rs` | CRDT state broadcast handler used by SWIM custom broadcasts. |
| `xtask/src/env/consumer.rs` | Local validation consumer gateway configuration. |
| `xtask/src/env/gateway.rs` | Local validation provider gateway configuration. |
| `xtask/src/env/operator.rs` | Local validation fixtures and overlay checks. |
