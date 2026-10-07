# Polling Cross-Site Load Signals

A gateway prefers the least-loaded peer serving a model, so it needs each peer's
load. Each gateway polls its peers over mutual TLS, records the readings in a
local store, and orders cross-site candidates from it. Wire format and store:
[Signals](signals.md). Selection: [Routing](routing.md), [Scoring](scoring.md).
Peer identity: [Authentication](auth.md).

## The Poll Path

Polling is direct per peer, not through a relay, and readings are attributed to
one verified identity.

| Step | What happens |
|---|---|
| Dial | Mutual TLS to the peer's `/v1/site/signals`, both ends presenting Grid site certificates. |
| Verify | The certificate is checked against the Grid CA and its SPIFFE identity, then the verified identity is compared to the site the poller dialed. A valid Grid peer answering for another site is refused. |
| Read | Under a byte ceiling and a time bound, so a slow or oversized peer cannot hold the poll open or exhaust memory. |
| Store | Keyed on the verified identity, never on anything the body carries, so no peer can inject readings as another. A body label disagreeing with the verified owner is dropped. |
| Bound | Only the contract names and EPP pool averages the gateway routes on, a DNS-1123 `grid_provider`, a finite non-negative value (at most one for `grid_provider_ready` and `grid_provider_error_ratio`), and the peer's first 64 providers by name. Custom `signalNames` are dropped. Refusals count in `grid_peer_signals_refused_total{peer,reason}`. |

A gateway polls only while serving, since one that is not serving has no routing
decision to inform. Live connections are proportional to the serving gateways,
not to the grid.

Poll and route meet at the store, and only there. Ordering runs off the request
path, reading each candidate's recent worst load into a least-loaded-first list.
The request path reads one ordered snapshot and picks among its healthy sites with
room, or failing that the best-scored sites not full. It does not read raw signals or
compute load. It reads resolved order.

## Provider Readiness

Each operator publishes whether its providers can serve, as
`grid_provider_ready{grid_site,grid_provider}`. A provider is not ready when its
EPP reports zero ready endpoints for two scrapes running and recorded no engine
answer in 30s, when no scrape succeeded within `staleMetricsSeconds` (half the
signal TTL when unset), or when it is `Unavailable`. The EPP counts endpoints
with fresh metrics, so a saturated engine can read zero while serving, which is
why two scrapes and the engine answer both gate the verdict.

A scrape answering without the pool's ready-endpoint series leaves readiness
unknown, not false, and does not exclude: a provider pointed at vLLM's own
`/metrics` carries no such series.

The verdict is also the provider's `Ready` condition, whose reason names the
cause and, for a failed scrape, its class. See [crds.md](./crds.md).

The gateway does not read this verdict. It reads the EPP's ready-endpoint count the
operator relays, and a site whose latest count is zero, or whose per-unit series stopped
while that count kept stamping, leaves selection within one poll and rejoins within one
poll of recovery. The serving config carries the same verdict for local
providers as `admission: none`. When every candidate for a model is excluded the
gateway answers 503 with `Retry-After`, not 404: the model exists but cannot be
served now. At the default 5s scrape and 5s poll, exclusion takes about 15s and
rejoin about 10s.

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
Point `metricsConfig.metricsEndpoint` at the EPP Service: a pod or headless
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

The gateway learns each site's ceiling, the most in-flight it has held with nothing queued,
and reads saturation (rho) as in-flight over that ceiling, smoothed per new sample. A site has
room while rho is below 1. Among healthy sites with room, three or more are picked two by
ceiling taking the lower rho, two are picked between weighted by ceiling over 1 + rho, one is
taken. With no site that has room, the pick is by ceiling among the sites not full, tied on the
best queue depth. A cluster praxis reports with no healthy endpoint is never picked while a
healthy one is left.

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
