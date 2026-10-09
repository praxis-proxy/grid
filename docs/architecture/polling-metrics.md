# Polling Cross-Site Load Signals

A gateway prefers the least-loaded site serving a model, so it needs each site's
load. Each operator polls its peers over mutual TLS and relays what it collected;
each gateway polls its local operator, records the readings in a local store, and
orders cross-site candidates from it. Wire format and store:
[Signals](signals.md). Selection: [Routing](routing.md), [Scoring](scoring.md).
Peer identity: [Authentication](auth.md).

## The Poll Path

The operator polls each peer directly and attributes readings to one verified
identity. The gateway reads the operator's relay over the same mutual TLS.

| Step | What happens |
|---|---|
| Dial | Mutual TLS to the peer's `/v1/site/signals`, both ends presenting Grid site certificates. |
| Verify | The certificate is checked against the Grid CA and its SPIFFE identity, then the verified identity is compared to the site the poller dialed. A valid Grid peer answering for another site is refused. |
| Read | Under a byte ceiling and a time bound, so a slow or oversized peer cannot hold the poll open or exhaust memory. |
| Store | Keyed on the verified identity, never on anything the body carries, so no peer can inject readings as another. A body label disagreeing with the verified owner is dropped. The gateway keys on the body's site label only for rows its own operator served, since that operator stamped each from the leaf it verified. |
| Age | A row's age is the response `Date` minus the row's stamp, both from one reading of the sender's clock, so the offset between two sites' clocks never enters. The operator keeps each relayed row's age and stamps it again when it serves its gateway, with a `Date` from that same reading, so a step of its wall clock moves the stamps and the `Date` together. A row stamped more than a second past its `Date` (the `Date`'s own resolution) or more than a day before it is dropped rather than read as current: the operator counts it in `grid_peer_signals_refused_total{peer,reason="age"}` and the gateway in `grid_signals_ingest_dropped_total{reason="skew"}`, where `skew` means a stamp out of line with its own `Date`, not an offset between sites. The gateway holds each row on its own timeline, its wall clock at start advanced by the monotonic clock only, so a step of the gateway's clock neither drops new rows as older than the held ones nor ages the held ones at once. |
| Bound | The operator serves its own gateway the contract series only, cuts each target at 4096 lines and holds the body under 768 KiB, dropping whole targets past it and counting them in `grid_signals_relay_truncated_total{store,cut}`, so a flood from one provider costs the last targets rather than every site. The gateway then keeps only the contract names and EPP pool averages it routes on, a DNS-1123 `grid_provider`, a finite non-negative value (at most one for `grid_provider_ready` and `grid_provider_error_ratio`), and the peer's first 64 providers by name. Custom `signalNames` are dropped. Refusals count in `grid_peer_signals_refused_total{peer,reason}`. |

A gateway polls only while serving, since one that is not serving has no routing
decision to inform. Each gateway holds one connection, to its operator, so live
WAN connections are proportional to the operators, not to the gateways.

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
served now. At the default 5s scrape and 5s poll, exclusion of a local site takes
about 15s and rejoin about 10s. A remote site's reading reaches the gateway through
its operator's peer poll, so its exclusion and rejoin also wait on that poll and its
budget: about 15s more at a 5s peer poll, and up to the 30s default poll plus its 10s
budget otherwise.

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
