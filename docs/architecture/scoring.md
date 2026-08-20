# Provider Scoring

For the complete relationship between scoring, routing groups, and request-time
selection, see [Provider Selection and Load Balancing](provider-selection-and-load-balancing.md).
Scores influence candidate ordering; they are not traffic weights and do not
split selection groups.

Grid scores provider pools when the operator renders a Praxis routing overlay.
Praxis reads that overlay from memory at request time; it does not call Grid,
Kubernetes, the operator, or an EPP metrics endpoint on the request path.

## Responsibility Boundary

Grid and llm-d make different decisions:

```text
Grid operator (before the request)
  EPP pool metrics -> select and score a provider pool -> routing overlay

Praxis edge (during the request)
  local overlay -> select an eligible provider gateway

llm-d EPP (inside the selected provider)
  request + endpoint state -> select an inference pod
```

Grid uses provider-level telemetry that can be collected asynchronously. llm-d
EPP can additionally use request-specific information, such as how much of the
current prompt prefix is cached on each pod.

Grid therefore does not offer a `prefixAware` scoring strategy. A pool-average
KV-cache utilization metric measures capacity pressure; it does not prove that
the current request's prefix is cached. Prefix affinity belongs in EPP.

## Configuration

`GridNetwork.spec.scoringPolicy.strategy` selects one provider-level strategy.
This follows llm-d's independent-scorer model without exposing a plugin system
or an arbitrary matrix of weights in the Grid API.

When `scoringPolicy` is present, `strategy` is required. Omit the entire
`scoringPolicy` object to use the `noMetrics` default. This also makes manifests
using the removed `profile`/`weights` shape fail admission instead of silently
falling back to another strategy.

### No metrics (default)

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridNetwork
metadata:
  name: production
spec:
  scoringPolicy:
    strategy: noMetrics
```

Omitting `scoringPolicy` has the same effect. Every admitted candidate receives
zero dynamic score. "No metrics" does not mean "no policy": health, admission,
freshness, model compatibility, geography, selection tiers, session affinity,
and Praxis selection policy continue to apply.

Use this strategy for heterogeneous grids, external APIs such as OpenAI,
Anthropic, or Bedrock, and providers that do not expose comparable pool
telemetry. Equivalent providers can participate in the same Praxis selection
group without an unavailable metric creating an artificial preference.

### Queue depth

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridNetwork
metadata:
  name: production
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: queueDepth
```

The operator prefers the provider with the shortest normalized queue:

```text
score = 1 - normalized_queue_depth
```

This corresponds to the intent of llm-d's `queue-scorer`. Grid normalizes an
EPP pool-average queue count using `metricsConfig.queueCapacity` and clamps the
result to `0.0..1.0`.

### KV-cache pressure

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: kvCachePressure
```

The operator prefers the provider with the most available KV-cache capacity:

```text
score = 1 - kv_cache_utilization
```

This corresponds to llm-d's `kv-cache-utilization-scorer`. Lower utilization
scores higher. This strategy is about available capacity, not cache affinity.

## Why Strategies Are Not Combined

Queue depth and KV-cache pressure describe different operating objectives. A
weighted sum can hide which condition caused a provider to win and can produce
surprising crossover points. Grid keeps the normal configuration explicit: use
`noMetrics`, or choose the single signal that represents the deployment's
provider-selection goal.

llm-d supports weighted scorer composition because it performs fine-grained,
per-request endpoint scheduling. Grid deliberately exposes less complexity at
the cross-site provider layer. New strategies should be added only when Grid
has a real provider-level signal with defined freshness and normalization.

## Metrics Configuration

`InferenceProvider.spec.metricsConfig` enables Prometheus text-format scraping.
Use `metricsEndpoint` when metrics are exposed by an llm-d EPP service rather
than the inference endpoint:

```yaml
spec:
  endpoint: http://inference-pool.inference.svc:8000
  metricsConfig:
    metricsEndpoint: http://llmd-epp-metrics.inference.svc:9090
    path: /metrics
    timeout: 2s
    poolName: llama-70b-east
    queueCapacity: 64
    staleMetricsSeconds: 30
    signalNames:
      queueDepth: llm_d_router_epp_average_queue_size
      kvCacheUtilization: llm_d_router_epp_average_kv_cache_utilization
      healthy: llm_d_router_epp_ready_endpoints
```

| Field | Purpose |
|---|---|
| `metricsEndpoint` | Optional base URL for a dedicated metrics service. |
| `path` | Metrics path, default `/metrics`. |
| `timeout` | Per-scrape timeout. |
| `poolName` | Selects samples for the intended EPP pool. |
| `queueCapacity` | Normalizes an absolute queue count to `0.0..1.0`. |
| `signalNames` | Maps Grid signals to exporter metric names. |
| `staleMetricsSeconds` | Grace period for reusing the last successful local scrape. |

One `InferenceProvider` represents a schedulable pool. Grid does not rank the
individual vLLM pods in that pool.

## Missing and Stale Metrics

NaN and infinite samples are discarded. Ratio values received through remote
state are clamped before scoring. A local scrape failure may reuse the last
successful sample while it remains within `staleMetricsSeconds`.

A provider with no live metrics currently falls back to neutral signal values
(0.5 for ratio signals, `healthy = true`). This compatibility behavior means a
provider with missing telemetry can score competitively with a provider under
real pressure. Production deployments using `queueDepth` or `kvCachePressure`
should ensure every competing provider exposes fresh, comparable telemetry for
the selected signal. `noMetrics` does not require a metrics endpoint.

## Admission and Ordering

Admission remains a harder boundary than score:

| State | New requests | Existing sessions |
|---|---|---|
| `new_and_existing` | Allowed | Allowed |
| `existing_only` | Rejected | Allowed |
| `none` | Rejected | Rejected |

`routingPolicy` then determines how the selected score interacts with
geography:

- `geographyFirst` keeps a same-site provider ahead of a remote provider.
- `scoreFirst` allows the provider with the better selected signal to outrank
  a local provider.

Use `scoreFirst` when queue or KV pressure is intended to drive cross-site
selection:

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: queueDepth
```

The routing overlay still contains the total score and score breakdown. A
metric strategy has only its selected contribution; `noMetrics` has all-zero
contributions. This keeps the decision easy to explain.

## Scoring Is Not Request Distribution

Grid scoring determines provider preference and overlay ordering. It does not
perform a request-time control-plane lookup and does not itself guarantee a
traffic ratio. Praxis performs local selection, session affinity, retry, and
failover from its current overlay snapshot.

If multiple providers should actively receive new traffic, the overlay must
place them in the same eligible selection group or publish an explicit traffic
distribution contract that Praxis understands. Recalculating a rank every few
seconds is not a substitute for request-time load balancing.

## Current Limits

- No request-specific prefix affinity at the Grid layer.
- No independent remote metric sample timestamp yet.
- Stabilized admission is available through `spec.admissionPolicy`; it uses
  bounded pressure/recovery counters and hold-down timers, while omitting the
  field preserves instantaneous compatibility behavior.
- Ranking still has no independent score-switch margin or dwell timer; that is
  separate from provider admission and remains future work.
- Queue normalization depends on a correct `queueCapacity`.
- A network-wide metric strategy is appropriate only when competing providers
  expose comparable telemetry; use `noMetrics` for heterogeneous providers.
- Strategy changes alter the overlay on the next successful reconciliation.

See [Routing and Overlays](routing.md) for admission, selection, stale-candidate
retention, and overlay revision behavior.
