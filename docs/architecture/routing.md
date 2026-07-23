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
Routing overlay ConfigMap
        |
        v
Praxis consumer gateway
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
6. Server-side applies an overlay `ConfigMap`.

The overlay key is `grid-config.json`.

## Routing overlay format

The overlay is a compact JSON document consumed by Praxis:

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

Candidate fields:

| Field | Meaning |
|-------|---------|
| `kind` | Capability kind, currently `inference_model` for model routing. |
| `name` | Model or capability name matched by the consumer gateway. |
| `site` | Grid site advertising the capability. |
| `cluster` | Praxis load-balancer cluster identity used for upstream routing. |
| `fresh` | Whether provider status is considered fresh enough for normal routing. |
| `credential` | Optional. Secret reference for upstream authentication. Present only for `api_provider` or authenticated `cloud_managed` candidates. **Never contains the token value** — only the Kubernetes Secret locating information. |

### Credential field security contract

When `credential` is present on a candidate, the field contains only:
- `strategy`: the authentication mechanism (currently `"bearer_token"`)
- `secretRef.name` / `secretRef.namespace` / `secretRef.key`: Kubernetes Secret locating information

The token value is **never written into the overlay ConfigMap**. The `grid_route`
filter parses the field and makes it available to downstream filters, but does
not perform Kubernetes API calls or inject credentials itself.

Credential injection is handled by the final-hop gateway that makes the final
backend call.  For direct API-provider or cloud-provider fallback, the consumer
gateway is often also the final-hop gateway, so it mounts the Secret and Praxis
AI injects the credential before forwarding to the provider API.  For remote
Grid sites, provider backend credentials stay in the remote provider site or
provider-side component; the consumer gateway should not receive those provider
tokens.

Native file-backed injection requires the Praxis AI `grid_credential_inject`
filter.  Grid can render the overlay and generated config for that path today,
but runtime deployments must use a Praxis AI image that includes the filter.

## Candidate scoring and ordering

The operator orders candidates before writing the overlay. It uses
`scoring::score_backends` with provider configuration, optional live metrics,
and optional CRDT-propagated provider metrics.

`Unavailable` providers are excluded. `Degraded` providers remain in the
overlay with `fresh: false`. Providers with no live metrics use neutral metric
scores.

At request time, `grid_route` selects from this pre-sorted candidate list rather
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
request time: `grid_route` selects from the pre-rendered candidate list, and the
Praxis AI filter chain handles credential injection and upstream forwarding.

Current local validation uses mock API-provider endpoints. That proves the Grid
overlay and Praxis routing/credential-injection mechanics. It does not prove a
real external provider protocol such as OpenAI, Anthropic, Bedrock SigV4, or
Vertex OAuth2.

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

1. **`grid_route`** selects the candidate and writes the secretRef fields to
   in-process filter metadata: `grid.route.credential.strategy`,
   `grid.route.credential.name`, `grid.route.credential.namespace`,
   `grid.route.credential.key`.  No token value is written.

2. **`grid_credential_inject`** reads those metadata keys, looks up the
   matching token in its configured credential map, and injects
   `Authorization: Bearer <token>` into the upstream request.

Consumer config filter chain ordering:

```text
grid_route              → selects candidate; writes credential metadata
grid_credential_inject  → reads credential metadata; injects Authorization
load_balancer           → upstream cluster selection with injected headers
```

This filter chain requires a Praxis AI image that includes
`grid_credential_inject`.  Grid renders the overlay and generated config shape;
the runtime image must provide the filter implementation.

### File-backed token source

In the current xtask validation mode for direct API-provider fallback, the token
value is resolved from a Kubernetes Secret by the xtask harness and written into
a Kubernetes Secret in the consumer cluster.  The consumer pod mounts that
Secret as a file, and `grid_credential_inject` reads the token from its
configured `file:` path at filter construction time.

In production, the same rule applies at the final-hop point: mount the Secret
only into the final-hop gateway or provider-side component that makes the final
backend call. Grid does not copy Secret values across clusters.

The token does NOT appear in:

- The Grid operator overlay `ConfigMap` (JSON).
- The `grid_route` filter candidates YAML.
- The consumer Praxis `ConfigMap`.
- The `grid.route.*` in-process filter metadata.
- Tracing spans or log lines.
- HTTP error response bodies.

### Deployment ownership

The operator generates the consumer Praxis config including the `grid_credential_inject`
section for direct API-provider routes.  Secret provisioning — creating,
rotating, and synchronizing the mounted credential Secret in the final-hop
cluster — is the responsibility of platform automation or an external Secret
manager.

The `grid_route` → `grid_credential_inject` filter chain interface is the same
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

At request time, `grid_route` matches the requested model against the
already-loaded overlay candidates, then chooses from Grid's pre-rendered
candidate order.  It does not call Kubernetes, SWIM, or the operator, and it
does not recompute the full scoring formula per request.

The Praxis consumer gateway extracts request facts such as the requested model
and runs `grid_route` against the overlay. For model inference, the filter scans
for matching `inference_model` candidates and sets the selected Praxis upstream
cluster.

If no candidate matches, the request fails cleanly instead of falling through to
an unrelated backend.

## Provider gateway trust

Provider gateways terminate mTLS before forwarding traffic to local inference
infrastructure. When the gateway image includes the `peer_identity_trust` filter,
provider gateways verify the peer identity from the downstream client certificate
and reject untrusted peers with HTTP 403 before forwarding to local infrastructure.

The current development trust policy matches the peer certificate organization.
Production policies should prefer stronger identity binding such as certificate
digest pinning or SPIFFE-style identities.

## Provider-side request forwarding

After site selection, provider-side Praxis filters forward the request to local
inference infrastructure. A common llm-d-style path is:

```text
provider gateway
  -> ext_proc
  -> llm-d external processor
  -> endpoint_selector
  -> inference pod or service
```

Grid chooses the provider site. llm-d or the provider-local scheduler chooses
the concrete pod, GPU, or endpoint inside that site.

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
| `queue_depth` | `[0.0, 1.0]` (ratio) | **Must be pre-normalized by the exporter or recording rule**; raw integer queue counts are not accepted |

### Destination-normalized metrics preferred

Sites and clusters should normalize their own capability metrics where
possible: the Prometheus exporter (or a recording rule on the destination)
is responsible for converting raw queue depths to a `[0.0, 1.0]` ratio.
This is the preferred pattern because heterogeneous sites can adapt
normalization to their local context (different maximum queue depths,
different latency budgets).

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

## When grid-config.json regenerates

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

Rendering a new `ConfigMap` does not mean the gateway has loaded it.  Praxis
gateways do not automatically reload from a changed `ConfigMap` volume
mount — a pod restart, rollout, or explicit gateway reload is required.  See
[Consumer Config](consumer-config.md#reload-and-rollout).

## Relevant files

| File | Role |
|------|------|
| `operator/src/controller/grid_network.rs` | Reconcile loop wiring for metrics, CRDT snapshots, overlay rendering, and status. |
| `operator/src/resources/routing_overlay.rs` | Provider-to-candidate mapping, scoring input construction, and overlay JSON rendering. |
| `operator/src/resources/provider_metrics.rs` | Prometheus scrape and metric-name mapping for `metricsConfig`. |
| `scoring/src/scoring.rs` | Six-signal backend scoring implementation. |
| `swim/src/state_broadcast.rs` | CRDT state broadcast handler used by SWIM custom broadcasts. |
| `xtask/src/env/consumer.rs` | Local validation consumer gateway configuration. |
| `xtask/src/env/gateway.rs` | Local validation provider gateway configuration. |
| `xtask/src/env/operator.rs` | Local validation fixtures and overlay checks. |
