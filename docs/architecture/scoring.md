# Scoring

Grid scores providers when the operator renders a Praxis routing overlay. The
result is a pre-sorted candidate list stored in the overlay `ConfigMap`; Praxis
then selects from that ordered list at request time.

## Current scoring path

The current scoring path is operator-side:

```text
InferenceProvider CRDs
  + optional metricsConfig scrapes
  + optional CRDT provider records
        |
        v
scoring::score_backends
        |
        v
ordered RoutingOverlay candidates
        |
        v
Praxis grid_route filter
```

The scoring engine is implemented in `scoring/src/scoring.rs`. The operator
uses it from `operator/src/resources/routing_overlay.rs` before applying the
overlay `ConfigMap`.

## Signals

The scoring engine combines six normalized signals:

| Signal | Meaning |
|--------|---------|
| Locality | Preference for nearer or local provider categories. |
| Queue depth | Lower pending queue is better. |
| KV-cache utilization | Lower cache pressure is better. |
| Prefix-cache hit ratio | Higher expected prefix reuse is better. |
| P99 latency | Lower tail latency is better. |
| Cost per token | Lower configured token cost is better. |

Signals without live values use the neutral score `0.5`. Providers with metrics
that report `healthy = false` are excluded by the scoring engine.

## Metrics input

`InferenceProvider.spec.metricsConfig` enables the operator to scrape a
Prometheus endpoint during `GridNetwork` reconciliation. The configured metric
names are parsed into `BackendMetrics` and attached to the matching provider
before scoring.

Providers without `metricsConfig`, providers with scrape failures, and signals
without configured metric names fall back to neutral values. `queueDepth` is
expected to be normalized to `0.0`–`1.0` by the exporter before Grid consumes it.

## Operator-side ordering

The operator builds a `GridState`, attaches provider metrics when available,
scores backends, and writes the resulting order into the overlay candidate list.
The overlay also carries a `fresh` flag:

| Provider phase | Included in overlay | `fresh` |
|----------------|---------------------|---------|
| `Available` | yes | `true` |
| `Pending` | yes | `true` |
| absent status | yes | `true` |
| `Degraded` | yes | `false` |
| `Unavailable` | no | — |

The `fresh` flag is consumed by `grid_route` as a request-time safety signal.
Unavailable providers are not emitted into the overlay.

## Request-time scoring

Praxis does not currently recompute the full six-signal formula for each
request. The `grid_route` filter consumes the operator-rendered overlay,
matches candidates by request attributes such as model name, and selects from
the pre-sorted candidates. A future Praxis-side `grid_scorer` filter can add
per-request scoring when request-local inputs are needed.
