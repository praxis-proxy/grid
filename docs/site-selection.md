# Cross-site site selection

How a grid gateway serving `gridServing` chooses a site for each request in
`grid_site_route`, what feeds the choice, and what you can change. The routing overlay and
the consumer `intelligent_route` filter are a separate path, described in
[routing.md](routing.md).

## Site choice

Each site's operator scrapes its EPP, polls every other site's operator, and relays
each series per provider under the site it verified. The gateway polls its local
operator, concludes what each site holds, learns each site's ceiling as the most
it has held with nothing waiting, and reads saturation (rho) as in-flight over that ceiling. A
site has room while rho is below 1. Among healthy sites with room, three or more are picked
two by ceiling taking the lower rho, two are picked between weighted by ceiling over 1 + rho,
one is taken. With no site that has room, the pick is by ceiling among the sites not full,
tied on the best queue depth. A site that is not ready, or whose cluster praxis reports with
no healthy endpoint, is chosen only when no healthy site is left.

With `availability.shedding` on, a model sheds once every healthy site serving it has been at
its ceiling with work waiting for `full_after_ms`, and routes again once no sample from a site
has shown that for `room_after_ms`. A shed request gets 429 with `Retry-After`. A model with
no healthy routable site gets 503.

## Inputs

| Input | Series | Source |
|---|---|---|
| Running per endpoint | `llm_d_epp_average_running_requests` | The EPP, relayed as is. |
| Waiting per endpoint | `llm_d_epp_average_queue_size`, `inference_pool_per_pod_queue_size` | The EPP, relayed as is. |
| Held before scheduling | `llm_d_epp_flow_control_queue_size` | The EPP, relayed as is. |
| Ready endpoints | `llm_d_epp_ready_endpoints` | The EPP, relayed as is. Zero reads as not ready. |

The gateway concludes in-flight as running plus waiting per endpoint, times ready endpoints,
plus what flow control holds. The operator's `grid_provider_*` series are its own conclusions
for Prometheus and dashboards; the gateway reads none of them.

Every input comes from the EPP's `/metrics`. Point `metricsConfig.metricsEndpoint` at the EPP
Service, since a pod address can reach a standby replica that reports nothing. The gateway
keeps no count of its own requests.

## Settings

Nothing needs setting. `availability.shedding` is the one switch, off by default. The other
fields of the filter's `availability` block, set from the chart's
`gridServing.siteRoute.availability`, have defaults, and their table and tuning order are in
[routing.md](routing.md#7b-measured-site-availability). On the operator,
`GRID_SIGNALS_SCRAPE_INTERVAL_SECS` (default 5) sets how often it scrapes its EPP, and
`metricsConfig.staleMetricsSeconds` how long a failing scrape keeps the last readiness.

## What to watch

`grid_route_site_ceiling`, `grid_route_site_rho` and `grid_route_site_weight` per site and
cluster; `grid_route_selections_total{path}` for which arm each request took;
`grid_route_decisions_total{site,reason}` for what was routed and what was refused and why;
`grid_route_shedding{model}` while a model is shed. Every routed response names its site and
cluster in `x-grid-site` and `x-grid-backend`.

The one poll every site's load arrives through: `grid_signals_poll_total{result}` (ok,
unreachable, unauthorized, too_large, no_date, bad_identity),
`grid_signals_last_success_timestamp_seconds`, `grid_signals_response_bytes` against the
gateway's 1 MiB read ceiling (the operator holds the relay under 768 KiB and counts what it
left out in `grid_signals_relay_truncated_total{store,cut}`), and `grid_signals_ingest_dropped_total{reason}` (skew,
site_mismatch, cap, parse) for rows the operator served that the gateway refused. The
gateway logs once when the poll starts failing and once when it recovers.

## Symptoms

| Symptom | Likely cause | Action |
|---|---|---|
| A site is never chosen | Zero ready endpoints, or its cluster has no healthy endpoint | Check `llm_d_epp_ready_endpoints` and the gateway's cluster health log line |
| Every remote site is unmeasured | The gateway's poll of its local operator is failing, or the operator is not collecting from peers | `grid_signals_poll_total` by result at the gateway; `grid_collection_up` and `grid_peer_signals_refused_total` at the operator |
| A site's ceiling sits far below what its engine runs | It was queued whenever it ran high, so no sample taught | Raise `explore_floor` |
| A small or slow site takes little | Expected: picks follow measured ceilings, and two choices never pick the busiest of three | Nothing |
| 429 under light load | Shedding is on and `full_after_ms` or `queue_full` is too low for the engine's normal queue | Raise them, or turn shedding off |
| A site answers errors fast and takes the most traffic | A refusing site never fills, so it reads as the site with the most room | Fix the site; passive ejection is a planned gate |
