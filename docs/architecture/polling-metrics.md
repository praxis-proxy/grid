# Polling Cross-Site Load Signals

A gateway prefers the least-loaded site serving a model, so it needs each site's
load. Each operator polls its peers over mutual TLS and relays what it collected;
each gateway polls its local operator, records the readings in a local store, and
orders cross-site candidates from it. Wire format and store:
[Signals](signals.md). Selection: [Routing](routing.md), [Scoring](scoring.md).
Peer identity: [Authentication](auth.md).

Backend-specific load metrics in this document describe the llm-d EPP integration.

## The Poll Path

The operator polls each peer directly and attributes readings to one verified
identity. The gateway reads the operator's relay over the same mutual TLS.

| Step | What happens |
|---|---|
| Dial | Mutual TLS to the peer's `/v1/site/signals`, both ends presenting Grid site certificates. |
| Verify | The certificate is checked against the Grid CA and its SPIFFE identity, then the verified identity is compared to the site the poller dialed. A valid Grid peer answering for another site is refused. |
| Read | Under a byte ceiling and a time bound, so a slow or oversized peer cannot hold the poll open or exhaust memory. |
| Store | Keyed on the verified identity, never on anything the body carries, so no peer can inject readings as another. A body label disagreeing with the verified owner is dropped. The gateway keys on the body's site label only for rows its own operator served, since that operator stamped each from the leaf it verified. |
| Age | A row's age is the response `Date` minus the row's stamp, both from one reading of the sender's clock, so the offset between two sites' clocks never enters. The operator keeps each relayed row's age and stamps it again when it serves its gateway, with a `Date` from that same reading, so a step of its wall clock moves the stamps and the `Date` together. A row stamped more than a second past its `Date` (the `Date`'s own resolution) or more than a day before it is dropped rather than read as current: the operator counts it in `grid_peer_signals_refused_total{peer,reason="skew"}` and the gateway in `grid_signals_ingest_dropped_total{reason="skew"}`, where `skew` means a stamp out of line with its own `Date`, not an offset between sites. The gateway holds each row on its own timeline, its wall clock at start advanced by the monotonic clock only, so a step of the gateway's clock neither drops new rows as older than the held ones nor ages the held ones at once. |
| Bound | The operator relays whole provider batches within a 4096-line target limit and a 768 KiB response limit. Each peer contributes up to 64 providers, with payload-bearing batches first. The gateway retains up to 256 provider identities per site and 4096 total, including metadata-only entries. Truncations count in grid_signals_relay_truncated_total and refused samples in grid_peer_signals_refused_total. |

A gateway polls only while serving, since one that is not serving has no routing
decision to inform. Each gateway holds one connection, to its operator, so live
WAN connections are proportional to the operators, not to the gateways.

Poll and route meet at the store, and only there. Ordering runs off the request
path, reading each candidate's recent worst load into a least-loaded-first list.
The request path reads one ordered snapshot and picks among its healthy sites with
room, or failing that the best-scored sites not full. It does not read raw signals or
compute load. It reads resolved order.

## Provider revisions

The operator publishes grid_provider_config_generation once per qualified
provider batch.
Its value is the InferenceProvider generation. The provider_uid label identifies
the object, including same-name recreations. Spec changes advance the generation.
Status-only updates preserve it.

For a batch with three load samples:

```text
grid_provider_config_generation{grid_site="b",grid_provider="m",provider_uid="u1",publisher_epoch="1",batch_sequence="11",batch_samples="3"} 7 2000
```

A changed UID, generation, or publisher epoch resets learned ceilings and their
load and congestion history. Missing or stale metadata disables learning while
fresh pressure signals remain usable. A zero-sample revision invalidates
learning. A newer complete batch starts fresh learning.

Receivers order batches by publisher epoch and sequence to reject delayed data.
Repeating a complete publication leaves its age and learning lifetime unchanged
and is not counted as a parse drop. At admission, hubs and gateways reject epochs
more than ten minutes ahead of their own wall clock, matching SWIM's lease bound,
before retaining any publication fence.
Relays preserve sample age and whole batches. Valid ordering metadata survives
rejected payloads. Ordering depends on preserving the site's SWIM high-water-mark
ConfigMap across restarts.

## Provider Readiness

Each operator publishes whether its providers can serve, as
`grid_provider_ready{grid_site,grid_provider}`. A provider is not ready when its
EPP reports zero ready endpoints for two scrapes running and recorded no engine
answer in 30s, when no scrape succeeded within `staleMetricsSeconds` (half the
signal TTL when unset), or when it is `Unavailable`. The EPP counts endpoints
with fresh metrics, so a saturated engine can read zero while serving, which is
why two scrapes and the engine answer both gate the verdict.

Provider scrapes run concurrently with a bound of 16. A network's readiness
floor is one scrape interval plus the longer of one interval or the latest
measured pass, and it overrides `staleMetricsSeconds` when longer. This keeps a
successful sample valid until the next non-overlapping pass can refresh it. The
trade-off is that a slow pass widens the window for every provider on the
network, so a real failure is reported later. The 3s local-exclusion claim
below therefore holds only when a pass finishes within one interval; scrape
cadence follows the slowest provider's timeout, which has no upper bound.

A scrape answering without the pool's ready-endpoint series leaves readiness
unknown, not false, and does not exclude: a provider pointed at vLLM's own
`/metrics` carries no such series.

The operator reads at most 1 MiB of the HTTP response body from one provider
/metrics endpoint. Reaching that budget is counted as `body_cap` and leaves
readiness unknown: the endpoint answered, so the provider is not declared down.
On EPP endpoints that Grid scrapes, use an EPP release that supports
`--fairness-id-metric-label-limit` (available in v0.10+) and set it to `0` so
the client-controlled `fairness_id` label collapses to `other`; otherwise the
`x-llm-d-inference-fairness-id` header can grow the exposition past the budget.

The verdict is also the provider's `Ready` condition, whose reason names the
cause and, for a failed scrape, its class. See [crds.md](./crds.md).

The gateway does not read this verdict. It reads the EPP's ready-endpoint count the
operator relays, and a site whose latest count is zero, or whose per-unit series stopped
while that count kept stamping, leaves selection within one poll and rejoins within one
poll of recovery. The serving config carries the same verdict for local
providers as `admission: none`. When every candidate for a model is excluded the
gateway answers 503 with `Retry-After`, not 404: the model exists but cannot be
served now. At the default 1s scrape and 1s poll, exclusion of a local site takes
about 3s and rejoin about 2s. A remote site's reading reaches the gateway through
its operator's peer poll, which is unchanged at 5s, so its exclusion and rejoin still
wait on that poll and its budget: about 15s more.

## Provider In-flight

Each operator also publishes `grid_provider_in_flight_requests`, beside
`grid_provider_ready_endpoints` so the count has a denominator at the reader.
Every input comes from the EPP's `/metrics`; nothing scrapes vLLM. The value is the larger of
two estimates, plus what the EPP's flow control holds for the pool
(`llm_d_epp_flow_control_queue_size`):

- the EPP's per-endpoint `llm_d_epp_inflight_requests`, summed, taking each
  endpoint's largest count across producer instances. Needs the EPP's
  inflight-load-producer.
- the pool's average running plus average queued, times ready endpoints.

The larger, so an EPP restart that zeroes its count does not make the site look
idle. The per-endpoint count carries no pool label, so an EPP serving several
pools falls back to the averages alone. A site with no fresh endpoint publishes
nothing, since its averages are frozen, and the gateway reads it as unknown.
Point `metricsConfig.endpoint` at the EPP Service: a pod or headless
address can reach a standby replica, which reports no series.

On a prefill/decode pool the EPP counts a request on both endpoints, so the value
measures endpoint occupancy, up to twice the requests.

The gateway does not read this series. It concludes its own in-flight from the raw EPP
series the operator relays, running plus waiting per endpoint times ready endpoints plus
what flow control holds, each instant read together.

## Provider Latency

Each operator also publishes recent latency from the EPP's request histograms
over 30s, only when at least 20 requests completed in that window, and never
borrowed from another site.

- `grid_provider_ttft_p50_seconds`, `grid_provider_ttft_p90_seconds`: time to
  first token, streaming requests, from `llm_d_epp_request_ttft_seconds`. Timed
  from receiving the request, so it includes flow-control wait and network.
- `grid_provider_tpot_seconds`: mean time per output token, streaming requests.
- `grid_provider_error_ratio`: failed over all requests. The latency histograms
  record only successes, so a failing site can read fast; this shows it.

The EPP labels these by model, not pool, so an EPP serving several pools reports
their combined latency. A restart resets its counters and the window starts over.

## Provider Series on /metrics

The operator exports these series on its Prometheus `/metrics` listener, labeled
`grid_site` and `grid_provider`, for its own providers and those it polls, so a
Prometheus scraping one hub sees every site. A peer's value there is what the hub
last polled, up to one poll old. A series the operator does not hold is absent,
not 0.

## Site Selection

The gateway learns a soft capacity bound from the most in-flight held with nothing queued and a
hard bound from the least held at a demonstrated limit. It reads a request count against the
lesser bound, never below the floor. Saturation (rho) is the nearer of that count and the EPP's
KV-cache mean plus spread, smoothed per new sample. A site has room while rho is below 1. Among
healthy sites with room, three or more are picked two by capacity taking the lower rho, two are
weighted by capacity over 1 + rho, and one is taken. With no site that has room, the pick is by
capacity among sites not full, tied on queue depth. A cluster praxis reports with no healthy
endpoint is never picked while a healthy one is left.

With `availability.shedding` on, a model sheds once every healthy site serving it has been
saturated for `full_after_ms` with work waiting (`queue_full` per serving unit, or any work
held before scheduling), and routes again once no sample from a site has shown that for
`room_after_ms`. A shed request gets 429 with `Retry-After` and an OpenAI-style
error; a model with no healthy routable site gets 503. The knobs and their defaults are in
`docs/routing.md`. The gateway keeps no count of its own requests in flight.

## Failure Behavior

A reading that is missing, stale, or never written because the peer was
unreachable, slow, or presented an untrusted or mismatched certificate scores the
candidate as maximally loaded, so it sorts after every candidate that has one.
The poll loop continues, so one unreachable peer does not wedge the others. A
peer reporting `grid_provider_ready 0` is excluded outright until a later reading
says 1, and if every candidate is excluded the model answers 503.

Loss of signal degrades to least preferred, not to idle. A drained burst stays
penalized until it ages out rather than snapping back on the first missing
sample.

## Gateway rejection reports

After each committed successful ingest and routing refresh, the gateway reports
its `grid_signals_ingest_dropped_total{reason="skew"}` evidence to its local
operator over the authenticated signals channel. Reporting has its own bounded
exchange and does not turn a successful scrape into a failed scrape. See the
[feedback wire contract](signals.md#gateway-ingest-feedback).

`grid_signals_feedback_failures_total{reason}` counts each failed delivery
attempt once. Its bounded reasons are `timeout` for an expired exchange budget,
`transport` for connection, authentication, or HTTP errors, `status` for a
non-204 response, and `acknowledgment` for missing, repeated, or mismatched
acknowledgment metadata. A successful delivery or a cycle with no pending
report adds no failure. Failed deliveries retain evidence for retry and do not
fail scrapes; report contents never become metric labels.

A bad observation is retained until its exact report is acknowledged. Later
clean cycles cannot replace an unacknowledged bad report. New rejection can
supersede an unacknowledged clean report; a delayed acknowledgment for that old
clean cannot clear the newer bad evidence. Failed fetches create no clean
observation, but can retry pending bad evidence. The highest sequence and
counter values latch rejected evidence instead of wrapping.

The random process epoch, sequence, cumulative count, and pending evidence
survive committed poller and certificate reloads. A staged or rejected reload
publishes nothing. The reporter folds a committed observation before refresh
and cancellation, so replacing a poller during refresh cannot lose a genuine
skew drop. Feedback is scoped to the verified local operator identity.

The operator exposes `GatewayAgesRejected` independently of peer diagnostics.
Its [status and recovery rules](crds.md#gateway-age-evidence) preserve unresolved
replica evidence across silence, replacement, and operator restart. These
conditions explain discarded signals; they do not alter network phase or add a
routing failure mode.
