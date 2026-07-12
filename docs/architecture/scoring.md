# Backend Scoring Model

## Current Scoring (OP-01)

Scoring is performed by the Praxis `grid_route` filter
reading a static candidate list rendered by the Grid
Operator. No per-request metrics are used.

**Two signals are active:**

| Signal | Effect |
|--------|--------|
| Stale candidate (`fresh: false`) | −100 penalty |
| Candidate site equals `local_site` | +10 bonus |

The highest-scored candidate wins. Equal scores break
by config order (first candidate wins). A stale
candidate can still win if it is the only match for
a model, or if it is on the local site and all remote
candidates are also stale.

### Freshness

The operator sets `fresh` per `InferenceProvider`
status phase when rendering the overlay:

| Phase | Included in overlay | `fresh` |
|-------|---------------------|---------|
| `Available` | yes | `true` |
| `Pending` | yes | `true` |
| absent status | yes | `true` |
| `Degraded` | yes | **`false`** |
| `Unavailable` | **no** | — |

### Locality ordering

The operator sorts overlay candidates by `backendKind`
so that higher-priority backends appear first:

| `backendKind` | Locality score |
|---------------|---------------|
| `local` | 1.0 |
| `remote` | 0.5 (no region context) |
| `cloud_managed` | 0.2 |
| `api_provider` | 0.1 |

Because Praxis `grid_route` breaks ties by config
order, a locally-ordered candidate list naturally
prefers local backends without requiring per-request
scoring logic.

## Unhealthy Backend Exclusion

`InferenceProvider` resources with `phase: Unavailable`
are excluded from the overlay entirely.  The Praxis
circuit breaker independently excludes backends that
have exceeded their failure threshold.

---

## Target Design (OP-05+): Six-Signal Weighted Scoring

The sections below describe the intended scoring model
once CRDT-propagated metrics are wired into the
overlay pipeline.  **None of this is active today.**

### Six Signals (planned)

| Signal | Default Weight | Source | CRDT Type |
|--------|---------------|--------|-----------|
| Locality | 3.0 | config (region-aware) | — |
| Queue depth | 3.0 | Prometheus / CRDT | LWW Register |
| KV cache utilization | 2.0 | Prometheus / CRDT | LWW Register |
| Prefix cache hit ratio | 2.0 | Prometheus / CRDT | LWW Register |
| P99 latency | 2.0 | local measurement | — |
| Cost per token | 1.0 | config (static) | — |

### Scoring Formula (planned)

```text
score(backend) =
    w_loc   × locality_score
  + w_queue × (1 - queue_depth)
  + w_kv    × (1 - kv_cache_utilization)
  + w_cache × prefix_cache_hit_ratio
  + w_lat   × (1 - p99_latency / max_latency)
  + w_cost  × (1 - cost_per_token / max_cost)
```

All signals are normalized to 0.0–1.0 where higher is
better. Backends with no metrics default to 0.5.

### Locality Scoring (planned)

Region-aware locality table:

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

### Metric Staleness (planned)

When CRDT-propagated metrics have not been updated
within a threshold (assuming 5 s SWIM probe interval):

- **3 gossip intervals** (15 s): staleness penalty
  applied to the site's scores.
- **10 gossip intervals** (50 s): site treated as
  unknown capacity (default 0.5 for all signals).

### Weight Customization (planned)

Weights will be configured per `GridNetwork` and
passed to the Praxis overlay `ConfigMap`:

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

### Implementation path

**OP-05 (planned):** The metrics-to-snapshot freshness
loop will fold CRDT-propagated metrics (queue depth,
KV cache, latency) into the overlay, enabling richer
backend selection in `grid_route`.

**`grid_scorer` filter (planned):** A per-request
scoring filter in Praxis applying the full weighted
formula is a follow-on to OP-05 and is not yet
implemented in the Praxis PR stack.
