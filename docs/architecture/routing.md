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

The token value is **never written into the overlay ConfigMap**. Consumers resolve the token from the Kubernetes Secret at credential-injection time. The `grid_route` filter parses the field for forward compatibility and makes it available to downstream filters, but does not perform Kubernetes API calls or inject credentials itself.

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

**`crdt::ProviderState`** still carries only a monotonic `revision` counter, not
a wall-clock timestamp.  CRDT-storage-level GC remains deferred.

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
remains deferred.

### Not implemented: hard exclusion

The GC policy does not implement hard exclusion of all `fresh=false` candidates.
A `fresh=false` candidate is only evicted after the TTL boundary; it is
**deprioritized**, not blocked.  See the scoring section for how `fresh=false`
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
cloud-managed backend; the category describes the operational boundary, not a
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
The difference is the backend category and credential boundary:

1. An `InferenceProvider` declares `backendKind: api_provider`.
2. The operator includes the API provider as a candidate when it is available.
3. Scoring normally places self-hosted candidates ahead of API-provider
   candidates, so API providers are used as fallback or explicit lower-priority
   routes.
4. Praxis applies credential injection before forwarding the request to the
   provider endpoint (see "Credential injection" below).
5. If no self-hosted candidate is available for a model, the API-provider
   candidate can become the selected route.

The fallback decision is therefore still local to the consumer gateway at
request time: `grid_route` selects from the pre-rendered candidate list, and the
Praxis filter chain handles credential injection and upstream forwarding.

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

The native injection path uses two AI-side filters in sequence:

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

### File-backed token source

In the current xtask validation mode, the token value is resolved from a
Kubernetes Secret by the xtask harness and written into a Kubernetes Secret in
the consumer cluster.  The consumer pod mounts that Secret as a file, and
`grid_credential_inject` reads the token from its configured `file:` path at
filter construction time.

The token does NOT appear in:

- The Grid operator overlay `ConfigMap` (JSON).
- The `grid_route` filter candidates YAML.
- The consumer Praxis `ConfigMap`.
- The `grid.route.*` in-process filter metadata.
- Tracing spans or log lines.
- HTTP error response bodies.

### Future production path

The remaining production work is ownership and lifecycle, not request-path
injection mechanics:

- **Operator-owned consumer config** — the operator generates the full consumer
  Praxis config including the `grid_credential_inject` section.
- **Consumer-cluster Secret provisioning** — the operator or an external Secret
  manager creates, rotates, and synchronizes the mounted credential Secret.

The `grid_route` → `grid_credential_inject` filter chain interface remains the
same regardless of how the consumer-cluster Secret is provisioned.

## Consumer gateway selection

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

The `scoring::BackendMetrics` struct is the boundary between metrics ingestion
and the scoring engine.  The following table defines the normalization
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
in a future revision.

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

### Deferred: KV-cache affinity

Routing decisions based on KV-cache affinity (routing requests to backends
that already hold relevant KV-cache entries) are deferred until this
normalization contract is stable.  The current `kv_cache_utilization` signal
influences scoring but does not implement affinity-aware routing.

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
