# AI Grid Network Routing Guide

AI Grid Network (AGN) routes inference traffic across provider gateways using a **multi-dimensional policy** rather than a single load-balancing algorithm. This guide starts with the routing outcome you want, then shows how policy, scoring, groups, affinity, and selection mode work together.

This guide covers the routing overlay and the consumer Praxis `intelligent_route` filter. A grid gateway serving `gridServing` chooses sites with `grid_site_route` instead, from load each site publishes: see [Tuning cross-site site selection](site-selection.md).

The practical model is:

1. Determine which providers are eligible to receive the request.
2. Order eligible providers and form **selection groups**.
3. Reuse an existing provider when session affinity permits it.
4. Find the **first viable selection group**.
5. Apply the configured selection mode inside that group.
6. Forward the request to the selected provider gateway.
7. Let the provider-local serving stack, such as llm-d/EPP, make any separate pod-level or replica-level decision.

AGN makes control-plane decisions asynchronously and publishes a versioned routing overlay. Praxis loads that overlay and makes the final request-time provider choice locally. AGN, Kubernetes, Prometheus, and EPP are not consulted synchronously for every request.

```mermaid
flowchart TD
    Request["Request for a model/capability"]
    Eligible["Eligibility + admission"]
    Order["Routing policy + scoring"]
    Groups["Ordered selection groups"]
    Affinity{"Valid session binding?"}
    Reuse["Reuse bound provider"]
    Viable["First viable selection group"]
    Picker["Selection mode"]
    Provider["Selected provider gateway"]
    Backend["Provider-local serving stack"]

    Request --> Eligible --> Order --> Groups --> Affinity
    Affinity -->|yes| Reuse --> Provider
    Affinity -->|no| Viable --> Picker --> Provider
    Provider --> Backend
```

The most important distinction is:

> **Selection groups decide who is allowed to compete together. Selection mode decides how Praxis chooses among providers inside the active group. Scoring influences preference/order. Weights influence proportional selection. These are different controls.**

## Start with the routing need

| Routing need | Primary configuration | What it does |
|---|---|---|
| Keep traffic close and use remote capacity only as fallback | `routingPolicy: geographyFirst` | Creates locality-based priority groups. |
| Always choose the highest-ranked eligible provider | `selectionPolicy.mode: deterministic` | Picks the first provider in the active group. |
| Spread requests evenly | `selectionPolicy.mode: roundRobin` | Rotates new, unbound requests across providers in the active group. |
| Spread requests without a repeating sequence | `selectionPolicy.mode: random` | Uniform random choice within the active group. |
| Send more requests to providers with greater configured capacity | `selectionPolicy.mode: weightedRandom` + `placementPolicy.strategy: static` + provider `capacityWeight` | Samples providers proportionally within the active group. |
| Let providers in different sites actively share traffic | `routingPolicy: scoreFirst` | Allows fresh, admitted providers across sites to participate in one active group. |
| Prefer the provider with less queue pressure | `scoringPolicy.strategy: queueDepth` + usually `routingPolicy: scoreFirst` | Changes provider ranking based on asynchronously observed queue depth. |
| Prefer the provider with more free KV-cache capacity | `scoringPolicy.strategy: kvCachePressure` + usually `routingPolicy: scoreFirst` | Changes provider ranking based on provider-level KV-cache pressure. |
| Stop new sessions going to a pressured provider | Stabilized admission plus a pressure signal | Moves pressured providers to `existing_only`; requires an active scoring strategy and matching provider metric signal. |
| Keep an existing session on the same provider | Praxis AI `session_affinity` | Reuses the bound provider before running a new selection. |
| Change routing without restarting Praxis | AGN overlay publication + Praxis overlay hot reload | Atomically replaces the accepted routing snapshot. |

For the low-level overlay, revision, delivery, credential, and provider-hop contracts, see [Routing Architecture and Overlay Contract](architecture/routing.md).

For metric collection and normalization details, see [Provider Scoring](architecture/scoring.md).

Configuration snippets show the routing settings to add to an existing
deployment. Provider snippets are partial resources: retain the required
`backendKind`, `providerKind`, and `endpoint`, along with the capability,
site, trust, and authentication settings for your deployment. See the
[CRD Reference](architecture/crds.md) for complete resource examples.

If `selectionPolicy` is omitted, Praxis uses deterministic selection. Set it
explicitly when you want traffic sharing; `noMetrics` alone does not enable
round robin or weighted selection.
The `grid-site` Helm chart explicitly sets `roundRobin` for a new network
unless configured otherwise; it preserves an existing network's selection
policy on upgrade. Check the rendered resource when comparing Helm and direct
CR installations.

---

# 1. Selection groups: the foundation

A **selection group** is a priority and resilience boundary.

Providers in the same active group may share new traffic according to the configured selection mode. Providers in lower-priority groups are fallback capacity and do not participate while an earlier group remains viable.

```mermaid
flowchart LR
    Request["New request"] --> G0["Group 0: preferred providers"]
    G0 --> A["Provider A"]
    G0 --> B["Provider B"]
    G0 --> C["Provider C"]

    Request -. "only if Group 0 is not viable" .-> G1["Group 1: fallback"]
    G1 --> D["Provider D"]
    G1 --> E["Provider E"]
```

Think of this as two separate questions:

```text
Which group is active?
        ->
How do I select inside that group?
```

A 50/30/20 weighted policy in Group 0 does **not** mean 50/30/20 across Group 0 and all fallback groups. The weights apply only among eligible candidates in the first viable group.

Likewise, a provider with a very good score in a lower-priority group does not automatically join the active group when the routing policy keeps that group separate.

## `geographyFirst`: local-first selection groups

Use this when the need is:

> Keep requests on the closest healthy capacity and use farther capacity only when the closer tier cannot accept the request.

`geographyFirst` orders candidates by admission, locality, freshness, score, and deterministic tie-breakers. Groups are separated by admission state, locality tier, and freshness; scores only order providers within a group.

Upgrade note: omitting `selectionPolicy` means deterministic selection, but the
first candidate can still change when AGN's ordering changes, including when
freshness differs. If a fixed primary matters, set the policy explicitly and
compare the rendered routing overlay before and after an upgrade.

For providers with the same admission state and freshness, the locality tiers
look like this (group numbers are illustrative, not fixed locality IDs):

```text
Group 0  same-site healthy providers     <- active
Group 1  same-zone providers             <- fallback
Group 2  same-region providers           <- fallback
Group 3  cross-region providers          <- fallback
```

Example:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: local-first
spec:
  routingPolicy: geographyFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: roundRobin
```

With two eligible local providers, requests rotate between those local providers. A remote provider remains fallback while the local group is viable.

**Demonstrated by:**

- [Grid Combined-Site Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-combined-site): local provider preference, remote-provider fallback after local withdrawal, and recovery.
- [Grid Workload Inference Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-workload-inference): health/locality failover for cluster-local workloads.
- [Grid Regional Failover and Cloud Burst](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-cloud-burst): site-local groups, regional fallback, and an external overflow group.

## `scoreFirst`: cross-site active selection

Use this when the need is:

> Let fresh, admitted providers in different sites actively compete instead of treating locality as a hard fallback boundary.

`scoreFirst` groups providers by admission state and freshness, regardless of site. Fresh providers admitted for new work can therefore share one group across sites. Scores affect their ordering; locality becomes a tie-breaker rather than a group boundary. Stale or existing-session-only candidates remain in separate groups.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: cross-site-active
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: roundRobin
```

This produces equal request selection across fresh admitted providers in the active group, even when those providers are in different sites.

**Demonstrated by:**

- [Grid LLM-d Pool Metrics Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-llmd-pool-metrics): uses `scoreFirst` so a better provider score can outrank locality.

Experimental cloud-burst work explores explicit locality grouping. That work is
not part of the stable `GridNetwork` API; use the stable `routingPolicy` values
documented here for mainline deployments. See the
[experimental cloud-burst demo](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-cloud-burst)
for its current status and requirements.

---

# 2. Deterministic selection

Use this when the need is:

> Always send a new request to the highest-ranked eligible provider.

Deterministic mode selects the first provider after AGN has ordered the active group.

```mermaid
flowchart LR
    Agn["AGN-ordered active group"]
    Agn --> A["1. Provider A"]
    Agn --> B["2. Provider B"]
    Agn --> C["3. Provider C"]
    Request["New request"] --> Pick["deterministic"]
    Pick --> A
```

Configuration:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: strict-preference
spec:
  routingPolicy: geographyFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: deterministic
```

This is useful for:

- strict primary/preferred provider behavior;
- making AGN's score/order directly determine the selected provider;
- predictable primary/fallback behavior.

A particularly useful combination for load-sensitive preference is:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: least-pressured-first
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: queueDepth
  selectionPolicy:
    mode: deterministic
  metricsRefreshInterval: "10s"
```

AGN asynchronously ranks the provider pools. Praxis then chooses the first provider from the accepted snapshot. Praxis does not query EPP during the request.

**Demo coverage:** deterministic selection is a supported mode, but the current demo set does not have a demo whose sole purpose is deterministic selection. The load-aware metrics demo is useful for understanding the ranking input that deterministic mode can consume.

---

# 3. Round-robin selection

Use this when the need is:

> Spread new requests evenly and predictably across equivalent providers.

Round robin takes equal turns among eligible providers inside the first viable selection group.

```mermaid
flowchart LR
    R1["Request 1"] --> A["Provider A"]
    R2["Request 2"] --> B["Provider B"]
    R3["Request 3"] --> C["Provider C"]
    R4["Request 4"] --> A
```

Configuration:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: even-local-balancing
spec:
  routingPolicy: geographyFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: roundRobin
```

Important behaviors:

- It rotates **inside the active group only**.
- It does not mix lower-priority fallback groups into the rotation.
- It balances request selections, not token count, request cost, latency, or concurrent work.
- Session affinity is checked first, so established sessions can make observed traffic less than perfectly even.
- Each Praxis gateway maintains its own local round-robin state; gateways do not coordinate a global cursor.
- With multiple consumer gateways, aggregate proportions depend on each
  gateway's request rate, affinity bindings, restarts, and when it accepts a
  replacement overlay. Round robin is local state, not a grid-wide cursor.

**Demonstrated by:**

- [Grid Distributed Token Rate Limit Demo](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-distributed-token-rate-limit): admitted traffic rotates across west, central, and east provider gateways.
- [Grid Regional Failover and Cloud Burst](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-cloud-burst): round robin inside site-local groups and within the final external overflow group.

---

# 4. Random selection

Use this when the need is:

> Give each provider in the active group an equal chance without requiring a repeating sequence.

Random mode chooses uniformly among eligible candidates in the active group.

```mermaid
flowchart LR
    Request["New request"] --> Random["uniform random"]
    Random --> A["Provider A"]
    Random --> B["Provider B"]
    Random --> C["Provider C"]
```

Configuration:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: random-provider-grid
spec:
  routingPolicy: geographyFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: random
```

Important behaviors:

- Admission and selection groups are evaluated first.
- Every eligible provider in the active group has equal probability.
- Random state is local to each gateway.
- Session affinity is checked before random selection.

**Demo coverage:** there is not currently a dedicated random-selection demo in `praxis-proxy/demos` or `praxis-proxy/experimental`.

---

# 5. Weighted-random selection

Use this when the need is:

> Send more new requests to providers with greater configured capacity.

Weighted random samples providers according to explicit relative weights inside the active group.

```text
Provider A capacityWeight: 50
Provider B capacityWeight: 30
Provider C capacityWeight: 20

Expected long-run new-request distribution:
A ~ 50%
B ~ 30%
C ~ 20%
```

The values are relative weights, not guaranteed percentages.

```mermaid
flowchart LR
    Request["New unbound request"] --> Group["First viable group"]
    Group --> Weighted["weightedRandom"]
    Weighted --> A["A weight 50"]
    Weighted --> B["B weight 30"]
    Weighted --> C["C weight 20"]
```

GridNetwork:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: weighted-grid
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: weightedRandom
  placementPolicy:
    strategy: static
```

Provider A (partial resource):

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: InferenceProvider
metadata:
  name: provider-a
spec:
  gridNetworkRef: weighted-grid
  capacityWeight: 50
```

Provider B and C would use their own `capacityWeight` values.

Important behaviors:

- `capacityWeight` accepts integers from `1` through `1000` and defaults to
  `1` when omitted. AGN copies it directly to the overlay's relative
  `traffic_weight`; it does not normalize the value or convert it to a
  percentage.
- Static weighted selection does not derive weight from a score.
- Weights do not override eligibility, admission, affinity, or selection-group precedence.
- With `geographyFirst`, a high-weight remote provider is still fallback while a closer group is viable.
- With `scoreFirst`, providers from different sites can share the active group and participate in the same weighted draw.
- Statistical results converge over sufficient traffic; a small sample should not be expected to match the configured ratio exactly.

**Demonstrated by:**

- [Grid Static and Dynamic Weighted Routing](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-weighted-dynamic-routing)

The demo explicitly distinguishes two statuses:

- **Static weighted selection:** merged/mainline behavior.
- **Metric-driven dynamic weighting:** experimental.

For the field definition and overlay contract, see the
[CRD Reference](architecture/crds.md) and
[Routing Architecture and Overlay Contract](architecture/routing.md).

---

# 6. Load-aware provider preference

Use this when the need is:

> Prefer provider pools with more available serving capacity.

There is **not** a `selectionPolicy.mode: loadAware`.

Load awareness is a separate scoring dimension. Mainline AGN currently exposes provider-level scoring strategies such as:

- `queueDepth`
- `kvCachePressure`

The scoring strategy changes provider score/order. The selection mode still determines how requests are chosen inside the active group.

```mermaid
flowchart LR
    Metrics["EPP/provider metrics"] --> AGN["AGN scoring"]
    AGN --> Rank["Provider order/rank"]
    Rank --> Groups["Selection groups"]
    Groups --> Picker["deterministic / RR / random / weighted"]
```

## Queue-depth preference

Use this when the need is:

> Prefer the provider pool with the shortest normalized queue.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: queue-aware
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: queueDepth
  selectionPolicy:
    mode: deterministic
  metricsRefreshInterval: "10s"
```

A provider also needs comparable metrics configuration; the following is a
partial `InferenceProvider.spec.metricsConfig` fragment:

The metric names below are an example mapping, not a universal llm-d metric
contract. Check the deployed exporter's `/metrics` output and configure the
exact names and pool labels it exposes. Names vary between scheduler versions
and metric adapters; a wrong name can leave AGN without the selected signal.

```yaml
spec:
  metricsConfig:
    metricsEndpoint: http://llmd-epp-metrics.inference.svc:9090
    path: /metrics
    timeout: 2s
    poolName: llama-70b-east
    queueCapacity: 64
    staleMetricsSeconds: 30
    signalNames:
      queueDepth: inference_pool_average_queue_size
      kvCacheUtilization: inference_pool_average_kv_cache_utilization
      healthy: inference_pool_ready_pods
```

Conceptually:

```text
score = 1 - normalized_queue_depth
```

Lower queue pressure produces the higher provider preference score.

## KV-cache pressure preference

Use this when the need is:

> Prefer the provider pool with more free KV-cache capacity.

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: kv-aware
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: kvCachePressure
  selectionPolicy:
    mode: deterministic
```

Conceptually:

```text
score = 1 - kv_cache_utilization
```

This is **provider-level capacity pressure**, not request-specific prefix-cache affinity. Request-specific prefix/cache-aware endpoint selection belongs in the provider-local inference scheduler such as llm-d/EPP.

## Scores are not weights

This is critical:

```text
score != traffic weight
```

If:

```text
Provider A score = 0.8
Provider B score = 0.4
```

that does **not** mean A receives twice as many requests.

For example:

```yaml
routingPolicy: scoreFirst
scoringPolicy:
  strategy: queueDepth
selectionPolicy:
  mode: roundRobin
```

can still give equal turns to A and B when they are both in the same active group.

If the desired behavior is "pick the least-pressured provider," pair score-driven ordering with `deterministic`.

If the desired behavior is "change proportional traffic share based on live pressure," that is **dynamic weighting**, which is a different capability.

**Demonstrated by:**

- [Grid LLM-d Pool Metrics Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-llmd-pool-metrics)

The demo has both `queueDepth` and `kvCachePressure` configurations with `routingPolicy: scoreFirst` and shows provider rank changing as pressure changes.

---

# 7. Dynamic pressure-based weighting - experimental

Use this when the desired need is:

> Keep several providers active but continuously reduce the traffic share of providers under greater pressure.

This differs from mainline metric scoring.

Mainline metric scoring changes **preference/order**.

Dynamic weighting changes **traffic share**.

```text
queue / KV pressure
        |
effective available capacity
        |
dynamic traffic weights
        |
weighted selection
```

The experimental three-pool demo uses the simplified model:

```text
available capacity = configured capacity * (1 - normalized pressure)
traffic share = provider available capacity / group available capacity
```

The demo is designed to exercise the end-to-end chain:

```text
reported metric
    |
calculated weight
    |
routing-overlay revision
    |
Praxis accepted/serving revision
    |
measured request distribution
```

**Status:** experimental. Do not document a pressure-weighted placement CRD as stable/mainline unless the current source and generated CRD schema confirm it.

**Demonstrated by:**

- [Grid Static and Dynamic Weighted Routing](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-weighted-dynamic-routing)

---

# 7b. Measured site availability

The gateway learns each site's ceiling, the most in-flight it has held with nothing waiting,
reads saturation as in-flight over that ceiling, and picks among the sites with room by it: two
choices by ceiling taking the lower saturation at three or more, a weighted pick between two.
With shedding on, a model whose every site has been at its ceiling with work waiting for
`full_after_ms` answers 429 with Retry-After until one site has room. Every input is a raw series the
site's operator relays from its EPP: running and waiting per endpoint, ready endpoints, and what
flow control holds. The gateway concludes in-flight from those itself and counts nothing of its
own. The operator's `grid_provider_*` conclusions are its Prometheus metrics, not gateway inputs.

Nothing needs setting. Every field below has a default, and the one switch is `shedding`.
The fields live in the `availability` block of the `grid_site_route` filter in the gateway's
praxis config, rendered from the chart's `gridServing.siteRoute.availability`, and
`examples/gateway/grid-site-route.yaml` shows the block. A change is a chart value and a rollout.

| key | default | what it does |
|---|---|---|
| `smoothing` | 0.3 | How far one new sample moves saturation toward the new reading. |
| `ceiling_half_life_ms` | 600000 | How long a learned ceiling takes to halve once load falls away. |
| `ceiling_floor` | 8 | The least a ceiling can be, so a quiet site does not read as full. |
| `explore_floor` | 0.25 | The least share of the largest ceiling a measured site weighs, so an unproven site can prove more. |
| `full_after_ms` | 5000 | How long a site stays at its ceiling with work waiting before it counts as full, the same for a site polled every 500 ms and one polled every 5 s. |
| `room_after_ms` | 2000 | How long no sample may show a site at its ceiling with work waiting before a shedding model routes again. |
| `queue_full` | 1.0 | Queued work per serving unit at which a site counts as full. Teaching stops on any whole request waiting, whatever this is set to. |
| `shedding` | false | Whether a full model answers 429. On only where every client retries on 429 and every provider publishes a queue. Keep it off where the EPP's flow control is on: two layers refusing on their own signals shed twice for one overload, and the grid's 429 would arrive while the EPP is still holding the request. |

Tune one metric against one truth: `grid_route_site_ceiling` should settle near the engine's
running plateau (far below means the site was queued whenever it ran high, raise
`explore_floor`); `grid_route_site_rho` should reach 1.0 as the engine queue forms and fall as
it drains (swinging, lower `smoothing`; lagging, raise it); `grid_route_selections_total{path}`
should be mostly `two_choices` (much `by_capacity` means the grid is full or `ceiling_floor` is
too low); with shedding on, `grid_route_shedding{model}` should engage as queues form and
release as they drain (late, shorten `full_after_ms`; flapping, lengthen `room_after_ms`).

# 8. Session affinity

Use this when the need is:

> Keep an established conversation or application session on the same provider while that provider remains eligible for existing work.

Session affinity is evaluated **before** a new provider selection.

```mermaid
flowchart TD
    Request["Request"] --> Key["Extract session key"]
    Key --> Existing{"Valid binding exists?"}
    Existing -->|yes| Bound["Reuse bound provider"]
    Existing -->|no| Group["Find first viable group"]
    Group --> Select["Apply selection mode"]
    Select --> Bind["Record successful binding"]
```

Session affinity belongs to the Praxis AI `intelligent_route` request-time configuration rather than the AGN scoring policy.

Example using a header:

```yaml
- filter: intelligent_route
  overlay_file: /etc/praxis/routing/routing-overlay.json
  session_affinity:
    enabled: true
    header: x-session-id
    ttl_secs: 3600
```

Example using a cookie:

```yaml
- filter: intelligent_route
  overlay_file: /etc/praxis/routing/routing-overlay.json
  session_affinity:
    enabled: true
    cookie: praxis-session
    ttl_secs: 3600
```

Behavior:

- Existing permitted binding: reuse the provider.
- No binding: run normal group + selection-mode logic and record the successful binding.
- Provider becomes `existing_only`: an already-bound session may continue when permitted, but new sessions are not placed there.
- Provider becomes excluded/ineligible: affinity cannot override the hard boundary; a fresh selection is required.

Because affinity is resolved first, round-robin or weighted traffic may not look exactly even when individual sessions generate different amounts of traffic.

Affinity entries are held in each Praxis process. A session key arriving at
another consumer gateway is not guaranteed to find the same entry. Use
appropriate ingress affinity when cross-request provider stickiness is required.

**Demonstrated by:**

- [Grid GLB Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-glb-demo): separate edge and provider affinity, provider withdrawal, recovery, and failback.
- [Grid Combined-Site Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-combined-site): existing-session behavior during local-provider withdrawal.

**Praxis AI reference:**

- [intelligent_route filter](https://github.com/praxis-proxy/ai/blob/main/docs/filters/intelligent_route.md)

---

# 9. Admission and failover

Use this when the need is:

> Stop placing new work on a provider that is unhealthy or under sustained pressure without necessarily breaking existing sessions immediately.

Admission is a harder boundary than score or weight.

Conceptually:

| Admission semantics understood by Praxis | New requests | Existing affinity |
|---|---:|---:|
| `new_and_existing` | yes | yes |
| `existing_only` | no | yes, when permitted |
| `none` | no; removed from eligible candidates | no |

Current AGN removes excluded providers before publishing its overlay. The
`none` value remains part of the generic Praxis overlay contract; AGN does not
emit it for excluded candidates today.

Neither a high score nor a high static weight can make an excluded provider eligible.

```mermaid
stateDiagram-v2
    [*] --> NewAndExisting
    NewAndExisting --> ExistingOnly: pressure/admission restriction
    ExistingOnly --> NewAndExisting: sustained recovery
    NewAndExisting --> Excluded: hard health failure
    ExistingOnly --> Excluded: hard health failure
    Excluded --> NewAndExisting: healthy + admitted
```

Selection groups turn this admission behavior into structured failover.

To opt into stabilized admission for configured metrics, set the policy
explicitly. If `pressure` is omitted, AGN uses the six values shown below as
defaults. If `pressure` is supplied, all six fields are required; durations
must be positive whole seconds (for example, `10s`). This example sets them
explicitly so the transition behavior is reviewable:

```yaml
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: stabilized-admission
spec:
  scoringPolicy:
    strategy: queueDepth
  admissionPolicy:
    mode: stabilized
    missingMetrics: existingOnly
    pressure:
      enterThreshold: 0.85
      exitThreshold: 0.70
      failureThreshold: 2
      successThreshold: 3
      minimumStateDuration: 10s
      recoveryHoldDown: 30s
---
apiVersion: grid.praxis.fast/v1alpha1
kind: InferenceProvider
metadata:
  name: queue-aware-provider
spec:
  gridNetworkRef: stabilized-admission
  providerKind: self_hosted
  backendKind: local
  endpoint: http://inference.example.svc:8080
  models:
    - name: example-model
  metricsConfig:
    metricsEndpoint: http://metrics.example.svc:9090
    path: /metrics
    queueCapacity: 64
    signalNames:
      queueDepth: inference_pool_average_queue_size
```

`missingMetrics` can instead be `excluded`. When `admissionPolicy` is omitted,
AGN retains its instantaneous compatibility behavior. Hard health failure
still excludes a provider immediately; pressure hysteresis controls the
admission transition for otherwise healthy providers.

For pressure observations, also configure `scoringPolicy.strategy` on the
`GridNetwork` and the matching signal name in every participating provider's
`metricsConfig.signalNames`, with a reachable metrics endpoint. For raw queue
counts, set `queueCapacity` to normalize the value. For KV-cache pressure, map
`kvCacheUtilization`. Without an active strategy and matching provider signal,
AGN treats admission as not configured and leaves otherwise healthy
providers `new_and_existing`; the thresholds above alone do not enable
load-aware admission.

Example:

```text
Group 0: local AGN providers
Group 1: remote AGN providers
Group 2: external providers
```

A new request uses Group 0 while it has an eligible candidate. It falls through to Group 1 only when Group 0 cannot serve new work, and to Group 2 only when the earlier groups cannot.

**Demonstrated by:**

- [Grid Regional Failover and Cloud Burst](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-cloud-burst): local backend failure, site failure, pressure-driven `existing_only`, cross-site fallback, and external overflow.
- [Grid Combined-Site Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-combined-site): local withdrawal and remote failover.
- [Grid Workload Inference Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-workload-inference): explicit provider withdrawal and recovery.

The cloud-burst demo is an experimental integration demo. Its current behavior is **group fallback**, not gradual percentage-based cloud bursting.

Failover takes effect after health observation, reconciliation, overlay
delivery, and acceptance by Praxis. A request can still fail against a provider
in the previous snapshot during that interval. Group fallback does not imply
automatic retry of a request already sent upstream; retry behavior must be
configured separately.

---

# 10. Dynamic reload

Use this when the need is:

> Change routing state without restarting Praxis or putting AGN in the request path.

AGN publishes a new content-addressed overlay when provider state, configuration, metrics, or remote site state changes.

Praxis validates the new overlay and atomically swaps the accepted in-memory snapshot.

```mermaid
flowchart LR
    Change["Provider/config/metric change"]
    AGN["AGN reconcile"]
    Overlay["New overlay revision"]
    Delivery["ConfigMap / overlay-sync"]
    Praxis["Praxis validation"]
    Swap["Atomic snapshot swap"]
    Request["Next new request"]

    Change --> AGN --> Overlay --> Delivery --> Praxis --> Swap --> Request
```

Praxis AI overlay mode:

```yaml
- filter: intelligent_route
  overlay_file: /etc/praxis/routing/routing-overlay.json
  reload:
    enabled: true
    debounce_ms: 500
```

Key properties:

- In-flight requests continue using the snapshot they already loaded.
- New requests use the new snapshot after successful validation.
- Invalid updates retain the last-known-good in-memory snapshot.
- Overlay reload changes candidate/routing state, but it does not dynamically add arbitrary new load-balancer clusters or TLS endpoints that were never configured in the running pipeline.
- A Kubernetes `ConfigMap` projection must not use `subPath` for the watched overlay file because `subPath` bypasses the normal atomic projection update mechanism.

**Demonstrated by:**

- [Grid GLB Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-glb-demo)
- [Grid Combined-Site Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-combined-site)

For the full revision lifecycle (rendered, distributed, accepted, and serving), see [Routing Architecture and Overlay Contract](architecture/routing.md).

---

# 11. Common routing profiles

|Routing policy|Deterministic|Round robin / random|Weighted random|
|---|---|---|---|
|`geographyFirst`|Top-ranked provider in the closest viable group.|Share within the closest viable group.|Weighted share within the closest viable group.|
|`scoreFirst`|Top-ranked fresh, admitted provider across sites.|Share across fresh, admitted providers.|Weighted share in the active cross-site group.|

All modes honor eligibility, admission, and affinity. Weighted mode requires
static placement and provider `capacityWeight`; all modes act only inside the
first viable group.

The YAML blocks in this section are partial `GridNetwork.spec` fragments unless
marked otherwise; merge them into the existing resource rather than applying
them as standalone manifests.

These profiles show how the dimensions compose.

## Keep traffic local and balance evenly

Need:

> Use nearby healthy providers evenly. Use remote providers only as fallback.

```yaml
spec:
  routingPolicy: geographyFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: roundRobin
```

Result:

```text
closest viable group
    A -> B -> C -> A ...

remote groups
    unused until the closer group is not viable
```

Best demonstrations:

- [Grid Combined-Site](https://github.com/praxis-proxy/demos/tree/main/demos/grid-combined-site)
- [Grid Workload Inference](https://github.com/praxis-proxy/demos/tree/main/demos/grid-workload-inference)

## Cross-site active/active

Need:

> Let healthy providers in different sites share traffic.

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: roundRobin
```

Result:

```text
fresh + admitted providers across sites
            |
       one active group
            |
       equal selections
```

## Prefer the least-queued pool

Need:

> Route new requests to the provider pool with the best currently observed queue score.

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: queueDepth
  selectionPolicy:
    mode: deterministic
  metricsRefreshInterval: "10s"
```

Best demonstration:

- [Grid LLM-d Pool Metrics](https://github.com/praxis-proxy/demos/tree/main/demos/grid-llmd-pool-metrics)

## Static capacity-weighted active/active

Need:

> Give larger provider pools a larger share of new traffic.

```yaml
spec:
  routingPolicy: scoreFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: weightedRandom
  placementPolicy:
    strategy: static
```

Provider examples:

```yaml
# Provider A
# Partial InferenceProvider.spec fragment
spec:
  capacityWeight: 50
```

```yaml
# Provider B
# Partial InferenceProvider.spec fragment
spec:
  capacityWeight: 30
```

```yaml
# Provider C
# Partial InferenceProvider.spec fragment
spec:
  capacityWeight: 20
```

Best demonstration:

- [Grid static weighted provider qualification](../tests/e2e/topologies/grid-static-weighted/README.md)
  (stable, in-repository)
- [Grid dynamic weighted routing](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-weighted-dynamic-routing)
  (experimental pressure-derived weights)

## Sticky sessions with local-first routing

Need:

> Keep sessions stable while retaining local-first failover.

GridNetwork:

```yaml
spec:
  routingPolicy: geographyFirst
  scoringPolicy:
    strategy: noMetrics
  selectionPolicy:
    mode: roundRobin
```

Praxis AI:

```yaml
- filter: intelligent_route
  overlay_file: /etc/praxis/routing/routing-overlay.json
  session_affinity:
    enabled: true
    header: x-session-id
    ttl_secs: 3600
```

Best demonstration:

- [Grid GLB Demo](https://github.com/praxis-proxy/demos/tree/main/demos/grid-glb-demo)

---

# 12. Demo-to-capability map

The demos are runnable examples of the routing model. Check each demo's pinned
sources, prerequisites, and qualification results before treating it as release
evidence; experimental demos can require APIs absent from mainline AGN.

| Demo | Repository | Routing behavior it demonstrates |
|---|---|---|
| `grid-cloud-burst` | [experimental](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-cloud-burst) | Site-local selection groups, round robin, geography-first preference, queue-driven admission, regional fallback, external overflow, hot reload. Experimental integration. |
| `grid-weighted-dynamic-routing` | [experimental](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-weighted-dynamic-routing) | Static weighted random, three-provider proportional selection, hot reload, and experimental pressure-driven dynamic weights. |
| `grid-distributed-token-rate-limit` | [experimental](https://github.com/praxis-proxy/experimental/tree/main/demos/grid-distributed-token-rate-limit) | Round-robin provider selection after quota admission; request-time routing remains local to Praxis. |
| `grid-llmd-pool-metrics` | [demos](https://github.com/praxis-proxy/demos/tree/main/demos/grid-llmd-pool-metrics) | `queueDepth`, `kvCachePressure`, `scoreFirst`, dynamic provider re-ranking, pressure and recovery. |
| `grid-glb-demo` | [demos](https://github.com/praxis-proxy/demos/tree/main/demos/grid-glb-demo) | Session affinity, provider withdrawal/recovery, hot reload, and separation of edge selection from AGN provider selection. |
| `grid-combined-site` | [demos](https://github.com/praxis-proxy/demos/tree/main/demos/grid-combined-site) | Local-first routing, remote fallback, existing-session behavior, and recovery. |
| `grid-workload-inference` | [demos](https://github.com/praxis-proxy/demos/tree/main/demos/grid-workload-inference) | Workload-originated local-first routing, health-based failover, and recovery. |
| `grid-route53-edge-entry` | [demos](https://github.com/praxis-proxy/demos/tree/main/demos/grid-route53-edge-entry) | Independent public-edge selection and private AGN provider selection; useful for understanding routing-layer composition. |

---

# 13. What AGN routing does not mean

## AGN provider selection is not llm-d pod selection

AGN selects a provider gateway/pool.

The provider-local serving stack can then independently choose a concrete inference endpoint or replica.

```text
Client
  |
Praxis consumer
  |
AGN/Praxis provider selection
  |
Praxis provider gateway
  |
llm-d / EPP endpoint selection
  |
vLLM replica
```

Request-specific prefix-cache affinity belongs at the provider-local scheduler where request and replica state are available.

## A score is not a percentage

Scores influence order and preference.

Weights influence proportional selection.

## A selection group is not a weight bucket

Selection groups define priority and fallback boundaries.

The picker runs only inside the first viable group.

## `noMetrics` does not disable intelligent routing

`noMetrics` disables dynamic metric scoring. Eligibility, health, admission, locality, freshness, affinity, selection groups, and selection mode still apply.

## Load-aware routing is not a selection mode

There is no stable `selectionPolicy.mode: loadAware`.

Use scoring to express provider-level load preference.

Dynamic pressure-derived traffic weighting is a separate experimental capability.

## Cloud burst is not one selection mode

The experimental cloud-burst demo composes:

- eligibility/admission;
- locality groups;
- provider health;
- queue pressure;
- first-viable-group fallback;
- external provider groups;
- normal request-time selection inside the chosen group.

Its validated cloud transition is currently hard/group fallback, not a claim of gradual percentage-based cross-tier bursting.

---

# 14. Control plane versus request path

AGN and Praxis intentionally split responsibilities.

```mermaid
flowchart LR
    subgraph Control["AGN control plane"]
        Observe["Observe provider/site state"]
        Score["Score + admit + group"]
        Publish["Publish versioned overlay"]
        Observe --> Score --> Publish
    end

    subgraph Data["Praxis request path"]
        Load["Accepted snapshot"]
        Affinity["Affinity"]
        Group["First viable group"]
        Select["Selection mode"]
        Forward["Provider gateway"]
        Load --> Affinity --> Group --> Select --> Forward
    end

    Publish -. "async delivery" .-> Load
```

**AGN owns:**

- provider and site discovery;
- provider eligibility and admission;
- provider-level metric observation;
- score/order computation;
- selection-group construction;
- static traffic-weight publication;
- versioned overlay publication.

**Praxis AI owns:**

- accepted immutable routing snapshot;
- session affinity;
- first-viable-group resolution;
- deterministic, round-robin, random, and weighted-random selection;
- provider-cluster choice;
- atomic overlay hot reload.

**Provider-local inference systems own:**

- pod/replica-level endpoint scheduling;
- request-specific prefix/cache-aware scheduling;
- inference-engine internals.

The result is that sophisticated routing policy can change asynchronously
without introducing a distributed control-plane lookup into every inference
request.

---

# 15. Agent tool discovery overlays

AGN discovers MCP tools from `AgentToolProvider` resources and publishes them
as routing-overlay candidates. This is a control-plane discovery capability,
not a complete MCP request-routing pipeline.

## Discovery and propagation

1. An `AgentToolProvider` references a `GridNetwork` and an MCP endpoint.
2. The operator probes the endpoint and records advertised tool names in
   `status.discoveredTools`.
3. Each admitted tool produces an overlay candidate with
   `kind: "mcp_tool"`.
4. Tool provider state propagates to remote sites through SWIM and CRDT state.

Tool providers use a `tool/` prefix in their CRDT provider identity. This keeps
an `AgentToolProvider` distinct from an `InferenceProvider` with the same
Kubernetes name. Tool names travel in the SWIM broadcast extension rather than
the capability OR-set.

Tool candidates do not receive inference metrics, queue-depth scores, or
capacity weighting. Unavailable providers are excluded from overlays.

## Tool allowlist

When both `spec.tools` and `status.discoveredTools` are non-empty,
`spec.tools` acts as an allowlist. Only discovered names present in
`spec.tools` enter the overlay. An empty `spec.tools` exposes all discovered
tools.

## Access policy

Tool providers use the same `accessPolicy.siteSelector.matchLabels` mechanism
as inference providers. If the consumer site identity is unknown and
`matchLabels` is non-empty, evaluation fails closed and excludes the provider.

## Data-plane boundary

The operator-generated consumer Praxis config is model-oriented: it parses a
model field and configures inference clusters. It therefore projects only
`inference_model` candidates. A tool-only overlay cannot produce that consumer
config. If no inference candidates remain, the operator removes a previously
generated static consumer config; an already-running static consumer still
requires its documented restart or reload to discard in-memory routes.

Executing MCP tool calls requires a dedicated MCP-capable Praxis pipeline that
extracts MCP tool metadata and defines a reachable load-balancer cluster for
every selected tool candidate. The routing overlay can supply discovery input
to that pipeline, but AGN does not derive or install the MCP data-plane
configuration in this change.

---

# References

AGN:

- [Grid repository](https://github.com/praxis-proxy/grid)
- [AGN documentation index](README.md)
- [Routing Architecture and Overlay Contract](architecture/routing.md)
- [Provider Scoring](architecture/scoring.md)
- [CRD Reference](architecture/crds.md)

Praxis AI:

- [`intelligent_route` filter](https://github.com/praxis-proxy/ai/blob/main/docs/filters/intelligent_route.md)

Runnable demos:

- [Praxis demos](https://github.com/praxis-proxy/demos/tree/main/demos)
- [Praxis experimental demos](https://github.com/praxis-proxy/experimental/tree/main/demos)
