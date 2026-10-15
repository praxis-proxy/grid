# Cross-site site selection

How a grid gateway serving `gridServing` chooses a site for each request in
`grid_site_route`, what feeds the choice, and what you can change. The routing overlay and
the consumer `intelligent_route` filter are a separate path, described in
[routing.md](routing.md).

## Site choice

Each site's operator collects provider metrics, polls its peers, and relays
readings under the verified site identity. The gateway polls its local operator.
It learns a soft bound from the most held with nothing waiting and a hard bound
from the least held at a demonstrated limit. Saturation (rho) combines in-flight
load relative to that ceiling with direct KV-cache pressure.

Pressure tiers precede prefix affinity. Selection prefers queue-free sites with
room, then queued sites with room, then sites not marked full. If none remain,
all healthy sites are eligible. Entirely missing telemetry stays a fallback.
Affinity narrows only within the selected tier.

Within either tier with room, selection uses pressure when every
remaining site has fresh rho, even if capacity is unknown. With three or more
sites, it draws two by capacity and takes the lower
rho. With two, weights are capacity over 1 + rho, with each share bounded to
10–90 percent. Unknown capacity receives the mean known capacity, or equal weights
when all capacities are unknown. Missing rho uses capacity-weighted selection.
Queue-only and saturated selection retain sites tied on the least queue depth.
Selection excludes unready sites. A site demoted by gateway health checks is a last
resort if it remains admitted.

With `availability.shedding` on, a model sheds once every healthy site serving it has been at
its ceiling with work waiting for `full_after_ms`, and routes again once no sample from a site
has shown that for `room_after_ms`. A shed request gets 429 with `Retry-After`. A model with
no healthy routable site gets 503.

## Inputs

These load metrics describe the llm-d EPP integration.

| Input | Series | Source |
|---|---|---|
| Running per endpoint | `llm_d_epp_average_running_requests` | The EPP, relayed as is. |
| Waiting per endpoint | `llm_d_epp_average_queue_size`, `inference_pool_per_pod_queue_size` | The EPP, relayed as is. |
| Held before scheduling | `llm_d_epp_flow_control_queue_size` | The EPP, relayed as is. |
| Ready endpoints | `llm_d_epp_ready_endpoints` | The EPP, relayed as is. Zero reads as not ready. |
| KV-cache fullness | `llm_d_epp_average_kv_cache_utilization`, `llm_d_epp_std_dev_kv_cache_utilization` | The EPP, relayed as is. Mean and spread are joined per scrape. |

The gateway concludes in-flight as running plus waiting per endpoint, times
ready endpoints, plus what flow control holds. The operator adds one
grid_provider_config_generation row per qualified batch to scope learning to a
[provider revision](architecture/polling-metrics.md#provider-revisions).
Fresh pressure remains usable without this row, but learning stays disabled.

The EPP exposes load samples at /metrics. Point metricsConfig.metricsEndpoint at
the EPP Service. A pod address can reach a standby replica that reports nothing.
The gateway keeps no count of its own requests.

## Settings

Nothing needs setting. `availability.shedding` is the one switch, off by default. The other
fields of the filter's `availability` block, set from the chart's
`gridServing.siteRoute.availability`, have defaults, and their table and tuning order are in
[routing.md](routing.md#7b-measured-site-availability). On the operator,
`GRID_SIGNALS_SCRAPE_INTERVAL_SECS` (default 5) sets how often it scrapes its EPP, and
`metricsConfig.staleMetricsSeconds` how long a failing scrape keeps the last readiness.

## What to watch

`grid_route_site_ceiling_soft`, `grid_route_site_ceiling_hard`, `grid_route_site_ceiling`,
`grid_route_site_rho_reported`, `grid_route_site_rho_counted`, `grid_route_site_rho` and
`grid_route_site_weight` per site and
cluster; `grid_route_selections_total{path}` for which arm each request took;
`grid_route_decisions_total{site,reason}` for what was routed and what was refused and why;
`grid_route_shedding{model}` while a model is shed. Every routed response names its site and
cluster in `x-grid-site` and `x-grid-backend`.

The one poll every site's load arrives through: `grid_signals_poll_total{result}` (ok,
unreachable, unauthorized, too_large, no_date, bad_identity),
`grid_signals_last_success_timestamp_seconds`, `grid_signals_response_bytes` against the
gateway's 1 MiB read ceiling (the operator holds the relay under 768 KiB and counts what it
left out in `grid_signals_relay_truncated_total{store,cut}`), and `grid_signals_ingest_dropped_total{reason}` (skew,
site_mismatch, cap, parse) for rows the operator served that the gateway
refused. `skew` is a row stamped more than a second past the operator's `Date`
or more than a day before it. The gateway logs once when the poll starts
failing and once when it recovers.

## Symptoms

| Symptom | Likely cause | Action |
|---|---|---|
| A site is never chosen | Zero ready endpoints, or its cluster has no healthy endpoint | Check `llm_d_epp_ready_endpoints` and the gateway's cluster health log line |
| Every remote site is unmeasured | The gateway's poll of its local operator is failing, or the operator is not collecting from peers | `grid_signals_poll_total` by result at the gateway; `grid_collection_up` and `grid_peer_signals_refused_total` at the operator |
| A site's soft bound sits far below what its engine runs | It was queued whenever it ran high, so no queue-free sample taught it | Raise `explore_floor` |
| A small or slow site takes little | Expected: picks follow measured ceilings, and two choices never pick the busiest of three | Nothing |
| 429 under light load | Shedding is on and `full_after_ms` or `queue_full` is too low for the engine's normal queue | Raise them, or turn shedding off |
| A site answers errors fast and takes the most traffic | A refusing site never fills, so it reads as the site with the most room | Fix the site; passive ejection is a planned gate |
