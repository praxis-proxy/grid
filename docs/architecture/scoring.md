# Scoring and Routing Preference

Grid scores providers when the operator renders a Praxis routing overlay. The
result is an ordered candidate list stored in a content-addressed overlay
`ConfigMap`. Praxis uses that order at request time; it does not scrape provider
metrics or recompute Grid's six-signal score for every request.

## Decision Path

```text
InferenceProvider CRDs
  + local Prometheus-compatible metrics
  + remote provider metrics propagated through Grid
        |
        v
normalize and validate signals
        |
        v
scoring::score_backends
        |
        v
admission + routingPolicy ordering
        |
        v
versioned RoutingOverlay candidates
        |
        v
Praxis intelligent_route
```

The scoring engine is implemented in `scoring/src/scoring.rs`. The operator
connects metrics, admission, locality, and overlay ordering in
`operator/src/resources/routing_overlay.rs`.

## Signals

Higher total scores are better. Each candidate carries its total `score` and a
`score_breakdown` containing the weighted contribution from every signal.

| Signal | Default weight | Better value |
|---|---:|---|
| Locality | 3.0 | A provider nearer to the consuming site. |
| Queue depth | 3.0 | Less of the provider's queue capacity in use. |
| KV-cache utilization | 2.0 | Less cache-capacity pressure. This is utilization, not prompt-prefix affinity. |
| Prefix-cache hit ratio | 2.0 | More expected prefix reuse. |
| P99 latency | 2.0 | Lower tail latency. |
| Cost | 1.0 | Lower configured cost. |

The breakdown fields are weighted contributions, not raw metrics. For example,
with the default queue weight:

```text
queue contribution = 3.0 * (1.0 - normalized queue depth)
```

The breakdown makes an operator decision explainable without requiring the
demo, gateway, or observability stack to reimplement the scoring formula.

## Selecting the provider-level strategy

`GridNetwork.spec.scoringPolicy.strategy` selects the one provider-level signal
that Grid should use for dynamic metric scoring. It is intentionally not an
opaque blend of every available signal:

| Strategy | What a higher score means |
|---|---|
| `noMetrics` | Do not prefer a provider using dynamic metrics. Health, admission, freshness, locality, and request-time policy still apply. |
| `queueDepth` | The provider has less normalized queue pressure. This is the llm-d load-aware mode used by the pool-metrics demo. |
| `kvCachePressure` | The provider has more available KV-cache capacity, meaning lower utilization pressure. |

When `scoringPolicy` is absent, Grid uses `noMetrics`. The six weights in the
table above describe the scoring engine's legacy combined-weight capability;
they are not silently enabled by omitting the policy. Explicitly select a
strategy when provider metrics should move cross-provider ranking.

For example:

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: queueDepth
  metricsRefreshInterval: "5s"
```

`routingPolicy` and `scoringPolicy` answer different questions. The scoring
strategy calculates the dynamic metric score. `geographyFirst` or `scoreFirst`
then determines whether locality or that score is the primary ordering rule.
Admission and freshness remain higher-priority safety gates in both modes.

## Metrics Configuration

`InferenceProvider.spec.metricsConfig` enables Prometheus text-format scraping.
By default, Grid appends `path` to the provider's inference endpoint. Set
`metricsEndpoint` when metrics are exposed by a separate service, such as an
llm-d EPP:

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

The fields have distinct purposes:

| Field | Purpose |
|---|---|
| `metricsEndpoint` | Optional base URL for a dedicated metrics service. When absent, Grid uses `spec.endpoint`. |
| `path` | Metrics path, default `/metrics`. |
| `timeout` | Per-scrape timeout. |
| `poolName` | Selects samples whose `name` label identifies the intended pool. A configured pool that matches no routing signal is a failed scrape. |
| `queueCapacity` | Converts a raw queue count to a normalized ratio: `waiting / capacity`, clamped to `0.0..1.0`. Minimum value is `1`. |
| `signalNames` | Maps Grid signals to exporter-specific Prometheus metric names. |
| `staleMetricsSeconds` | Optional grace period for reusing the last successful local scrape. |

This contract lets Grid consume pool-level llm-d EPP metrics while keeping pod
selection inside llm-d. One `InferenceProvider` represents a schedulable pool;
Grid does not rank each vLLM pod in that pool.

### Normalization

Grid expects ratios in `0.0..1.0` for queue depth, KV-cache utilization, prefix
hit ratio, and error rate. A destination exporter or recording rule should
normally perform normalization because it knows the pool's capacity. The
`queueCapacity` adapter is available for EPPs that expose an absolute waiting
request count.

P99 latency is supplied as milliseconds and normalized by the scoring engine
against its current maximum-latency constant. Grid does not calculate a P99
gauge from histogram buckets during a scrape.

NaN and infinite samples are discarded. Ratio values received through remote
state are clamped before scoring.

## Admission Before Ranking

Admission is a harder boundary than score:

| Observed condition | Admission state | Effect |
|---|---|---|
| Healthy and below saturation thresholds | `new_and_existing` | Eligible for new and established sessions. |
| Queue above `0.85` or KV-cache utilization above `0.90` | `existing_only` | Preserve established sessions; do not admit new ones. |
| Explicitly unhealthy | `none` | Excluded from the overlay. |
| No metrics | `new_and_existing` | Current compatibility behavior; see limitations below. |

Candidate sorting always places `new_and_existing` ahead of `existing_only`
and removes `none` candidates.

## Routing Policies

`GridNetwork.spec.routingPolicy` controls whether locality or the weighted score
is the primary ranking input. The field is optional.

### `geographyFirst`

This is the default and preserves existing behavior:

1. admission state;
2. locality tier;
3. score, descending;
4. freshness;
5. deterministic `(site, name, cluster)` tie-break.

A same-site candidate remains ahead of a remote candidate regardless of metric
score, unless admission removes or restricts it.

### `scoreFirst`

This policy enables metrics-driven cross-site preference:

1. admission state;
2. freshness;
3. score, descending;
4. locality tier;
5. deterministic `(site, name, cluster)` tie-break.

A healthy remote pool can outrank a local pool when its total score is higher.
Use this mode only when the participating providers expose comparable,
normalized signals and the deployment accepts cross-site routing.

```yaml
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridNetwork
metadata:
  name: production
spec:
  routingPolicy: scoreFirst
```

The operator writes the final zero-based `rank`, total `score`, and
`score_breakdown` into each candidate. Praxis follows that ordering and applies
request-time model matching and session affinity.

## Freshness and Missing Values

Signals without values currently receive a neutral score of `0.5`. A local
scrape failure may reuse the last successful sample while it remains within
`staleMetricsSeconds`; after that grace period, it returns to neutral scoring.
The local cache is process memory and is cleared when the operator restarts.

Remote provider values are propagated through Grid state. SWIM freshness says
whether the advertising site is participating; it is not yet a timestamp for
the individual metrics sample.

These compatibility behaviors matter more under `scoreFirst`: neutral values
can make a provider with missing telemetry competitive with a provider that is
reporting real pressure. Production policy should not interpret missing data as
proof of spare capacity.

## Current Safety Limits

The following are explicit current limitations:

- Candidate ordering has no score-switch margin, observation count, dwell
  timer, or recovery hold-down. A small score change can change rank.
- Admission thresholds are point-in-time comparisons without hysteresis.
- Remote metric snapshots do not carry independent sample timestamps.
- Missing or expired metrics return to neutral scoring rather than failing
  closed.
- KV-cache utilization measures capacity pressure. It does not tell Grid that
  a particular request prefix is cached in a pool.
- Signals that an exporter does not expose remain neutral; Grid does not invent
  favorable values.

Until the deterministic routing-safety policy adds freshness, hysteresis, and
recovery controls, enable `scoreFirst` deliberately and monitor score changes,
scrape failures, overlay revisions, and actual provider attribution.

## Request-Time Boundary

Praxis does not recompute the full score. `intelligent_route` reads the
validated, pre-sorted overlay, matches candidates to request attributes such as
model name, and selects an eligible candidate. Per-request inputs such as an
explicit residency constraint or session binding must remain hard policy inputs
rather than being approximated by DNS or a weighted provider score.

See [Routing and Overlays](routing.md) for admission, locality, stale candidate
retention, and overlay revision details.
