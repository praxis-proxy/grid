# Demo Scenarios — 2026

Demo scenarios for the AI Grid, progressing from
simple multi-cluster routing to full grid operations
with metrics-driven failover.

## Demo 1: Basic Multi-Cluster Model Routing

**Setup**: Two clusters, each running llm-d with
different models.

- Cluster A: Granite 3.3 8B
- Cluster B: Llama 3.2 8B
- Both clusters in the same GridNetwork

**Flow**: A workload on Cluster A requests
`model: llama-3.2-8b`. The local cluster doesn't
have it, so the grid scores Cluster B (which does)
and routes the request there via mTLS. The response
returns transparently.

**Demonstrates**: Cross-cluster model-based routing,
mTLS data plane, grid scoring, SNI-based workload
access.

## Demo 2: API Provider Fallback

**Setup**: One cluster running llm-d + one third-party
API provider.

- Cluster A: Llama 3.2 8B (local, self-hosted)
- OpenAI API: GPT-4o (api_provider)

**Flow**: A workload requests inference. Scoring
prefers the local cluster (locality 1.0 vs 0.1).
The request goes to local llm-d. Then the local
backend is stopped. The next request falls back to
OpenAI with transparent credential injection.

**Demonstrates**: Locality-based scoring, circuit
breaker failover, credential injection for API
providers, transparent provider switching.

## Demo 3: Full Grid — Clusters + Cloud + APIs

**Setup**: Three clusters, one cloud service, two
API providers.

- Cluster A: Granite 3.3 8B (us-east-1)
- Cluster B: Llama 3.2 8B (eu-west-1)
- Cluster C: Consumer only (us-east-1)
- Bedrock: Claude via AWS (cloud_managed, us-east-1)
- OpenAI: GPT-4o (api_provider)
- Anthropic: Claude Sonnet (api_provider)

**Flow**: A workload on Cluster C (consumer) requests
inference. Scoring ranks: Cluster A (same-region
remote, locality 0.7), Cluster B (cross-region, 0.4),
Bedrock (cloud, 0.2), then API providers (0.1). The
request routes to Cluster A. Show the `x-grid-backend`
response header confirming the routing decision.

**Demonstrates**: Three-category backend support,
region-aware locality scoring, consumer-only sites,
multi-site GridNetwork formation.

## Demo 4: Metrics-Driven Load Balancing

**Setup**: Two clusters with the same model, different
load levels.

- Cluster A: Llama 3.2 8B (queue_depth: 0.9, saturated)
- Cluster B: Llama 3.2 8B (queue_depth: 0.1, idle)

**Flow**: With both clusters offering the same model,
locality is equal (both local or both same-region).
The queue_depth signal (weight 3.0) dominates. Cluster
B's low queue depth gives it a higher score. Requests
route to Cluster B.

Then Cluster B also saturates (queue_depth: 0.8).
Both clusters are now heavily loaded. The next request
falls back to the API provider (OpenAI) which has no
queue depth signal but is always available.

**Demonstrates**: Queue depth scoring, metrics-driven
routing, automatic spillover to API providers when
self-hosted capacity is exhausted, graceful
degradation.

## Demo 5: KV Cache Affinity

**Setup**: Two clusters with the same model.

- Cluster A: Llama 3.2 8B (prefix_cache_hit_ratio: 0.9)
- Cluster B: Llama 3.2 8B (prefix_cache_hit_ratio: 0.1)

**Flow**: A workload sends a series of requests with
the same prompt prefix. The first request routes based
on normal scoring. Cluster A's warm prefix cache
(hit ratio 0.9) gives it a scoring boost via the
prefix_cache weight (2.0). Subsequent similar requests
continue routing to Cluster A for cache affinity.

**Demonstrates**: Prefix cache scoring signal, cache-
aware routing, TTFT improvement from cache hits.

## Demo 6: Budget Enforcement

**Setup**: Two clusters + one API provider, budget
configured per tenant.

- Cluster A: Llama 3.2 8B (cost: $0.001/1k tokens)
- OpenAI: GPT-4o (cost: $0.03/1k tokens)
- Budget: $1.00 per hour for tenant "team-alpha"

**Flow**: Workloads send requests. The operator tracks
spend via G-Counter CRDTs. When the budget is 80%
consumed, the scoring filter constrains to cheaper
backends (Cluster A over OpenAI). When 100% consumed,
requests are rejected with a budget-exceeded error.

**Demonstrates**: G-Counter budget tracking, cascading
cost constraint, graceful degradation from expensive
to cheap backends.

## Demo 7: Site Join and Discovery

**Setup**: Two clusters already in a GridNetwork. A
third cluster joins.

- Cluster A + B: existing GridNetwork "production"
- Cluster C: new, deploying the Grid Operator

**Flow**: Cluster C creates a GridNetwork with Cluster
A as a seed. SWIM discovers A, then B (via A's
membership list). GridSite resources appear
automatically. mTLS certificates are exchanged.
Cluster C's GridSite status transitions:
Pending → Discovered → Connecting → Active. Once
Active, Cluster C's InferenceProviders are visible
to A and B.

**Demonstrates**: SWIM-based discovery, automatic
GridSite creation, mTLS certificate exchange, site
lifecycle state machine, capability propagation.

## Demo 8: MCP Tool Federation

**Setup**: Two clusters, each with different MCP tools.

- Cluster A: "database-query" tool
- Cluster B: "web-search" tool

**Flow**: An agent on Cluster A calls `tools/list`
through the Gateway. The response includes both
"database-query" (local) and "web-search" (from
Cluster B). The agent calls `tools/call` for
"web-search". The Gateway routes the request to
Cluster B's MCP server via mTLS. The result returns
transparently.

**Demonstrates**: MCP tool federation, cross-cluster
tool discovery, transparent tool invocation routing,
virtual MCP server on the Gateway.

## Demo 9: Failover Under Partition

**Setup**: Three clusters in a GridNetwork. Network
partition isolates Cluster C.

- Cluster A + B: connected (same region)
- Cluster C: partitioned (different region)

**Flow**: Before partition: all three clusters share
capabilities and metrics via CRDT gossip. After
partition: Cluster C's metrics go stale. After 3
gossip intervals (15s), the scoring filter applies a
staleness penalty. After 10 intervals (50s), Cluster
C is treated as unknown capacity. SWIM eventually
declares Cluster C suspect, then dead. GridSite
status: Active → Unreachable → Left.

Workloads on A and B continue routing between
themselves and API providers. Cluster C continues
operating independently with stale-but-available
state.

**Demonstrates**: Partition tolerance, staleness
detection, SWIM failure detection, graceful
degradation, stale-but-available routing.
