# Backend Scoring Model

The grid scoring engine ranks all healthy backends
for each request using a weighted multi-signal formula.
The highest-scoring backend is selected.

## Six Signals

| Signal | Default Weight | Source | CRDT Type |
|--------|---------------|--------|-----------|
| Locality | 3.0 | config (region-aware) | — |
| Queue depth | 3.0 | Prometheus / CRDT | LWW Register |
| KV cache utilization | 2.0 | Prometheus / CRDT | LWW Register |
| Prefix cache hit ratio | 2.0 | Prometheus / CRDT | LWW Register |
| P99 latency | 2.0 | local measurement | — |
| Cost per token | 1.0 | config (static) | — |

## Scoring Formula

```text
score(backend) =
    w_loc   × locality_score
  + w_queue × (1 - queue_depth)
  + w_kv    × (1 - kv_cache_utilization)
  + w_cache × prefix_cache_hit_ratio
  + w_lat   × (1 - p99_latency / max_latency)
  + w_cost  × (1 - cost_per_token / max_cost)
```

All signals are normalized to 0.0-1.0 where higher is
better. Backends with no metrics use a default score
of 0.5 for missing signals.

## Locality Scoring

Locality is region-aware:

| Backend Type | Same Region | Cross Region |
|-------------|-------------|--------------|
| Local cluster | 1.0 | 1.0 |
| Remote cluster | 0.7 | 0.4 |
| Cloud managed | 0.2 | 0.2 |
| API provider | 0.1 | 0.1 |

When region is unknown, Remote defaults to 0.5.

Locality is a scoring signal, not a hard constraint.
A remote cluster with dramatically better metrics can
outscore a congested local cluster.

## Unhealthy Backend Exclusion

Backends with `healthy: false` in their metrics are
excluded from scoring entirely. The circuit breaker
independently excludes backends that have exceeded
the failure threshold.

## Metric Staleness

When metrics propagated via CRDT have not been updated
within a threshold:
- **3 gossip intervals** (15s): staleness penalty
  applied to the site's scores
- **10 gossip intervals** (50s): site treated as
  unknown capacity (default 0.5 for all signals)

## Weight Customization

Weights are configured per `GridNetwork` and passed
to the Praxis overlay `ConfigMap`:

```yaml
scoring:
  weights:
    locality: 3.0
    queue_depth: 3.0
    kv_cache: 2.0
    prefix_cache: 2.0
    latency: 2.0
    cost: 1.0
```

## Where Scoring Runs

The scoring engine lives in the `scoring` crate.
For production routing, the scoring logic is compiled
into a Praxis filter (`grid_scorer`) that reads the
overlay config and scores backends per-request.

The Grid Operator generates the overlay config
(backend list, weights, metrics). Praxis reads the
config and executes the scoring formula in the
request-phase filter pipeline.
