# Metrics Reference

Every metric the grid exports, what it means, and where to scrape it. Two
components expose Prometheus endpoints and they answer different questions.

| Endpoint | Component | Serves |
|---|---|---|
| `/metrics` on `GRID_METRICS_ADDR`, default `0.0.0.0:9090` | Site operator | Reconciliation, probing, peer polling, and the provider readings this site publishes |
| `/metrics` on the gateway metrics listener, opt-in and TLS only | Grid gateway | Site selection, shedding, and serving-config reloads |

The operator's endpoint carries conclusions, not inputs. The raw provider series
a gateway routes on travel over `/v1/site/signals` instead, described in
[Signals](architecture/signals.md). A series the operator holds but does not name
below never reaches `/metrics`.

A metric with labels does not appear in a scrape until its first observation, so
an idle operator exports far fewer series than this page lists. Label values are
bounded: outcomes, phases, results, and peer or provider names, never addresses,
fingerprints, or certificate content.

## Operator: provider readings

The provider's own state, as this site measured it or as a peer published it.
Both labels are always present: `grid_site` names the site the reading came
from, and `grid_provider` the provider within it.

| Metric | Type | Meaning |
|---|---|---|
| `grid_provider_ready` | gauge | 1 when the provider can serve, 0 when not. |
| `grid_provider_ready_endpoints` | gauge | Endpoints behind the provider answering with fresh metrics. |
| `grid_provider_in_flight_requests` | gauge | Requests the provider holds, running, engine-queued, and held by flow control. |
| `grid_provider_ttft_p50_seconds` | gauge | Median streaming time to first token over the last 30s. |
| `grid_provider_ttft_p90_seconds` | gauge | 90th percentile streaming time to first token over the last 30s. |
| `grid_provider_tpot_seconds` | gauge | Mean streaming time per output token over the last 30s. |
| `grid_provider_error_ratio` | gauge | Failed requests over all requests in the last 30s. |

## Operator: scraping this site's providers

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_provider_scrape_total` | counter | `grid_provider`, `result` | Provider metrics scrapes by result. |
| `grid_provider_last_scrape_success_timestamp_seconds` | gauge | `grid_provider` | Unix time of the provider's last scrape with its ready-endpoint series. |
| `grid_model_discovery_total` | counter | `provider`, `outcome` | Served-model discovery polls by outcome. |

## Operator: polling peer sites

One poll per peer per interval, so every series here is per peer. The poll path
is described in [Polling Cross-Site Load Signals](architecture/polling-metrics.md).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_peer_poll_total` | counter | `peer`, `outcome` | Peer signal polls by outcome. |
| `grid_peer_poll_duration_seconds` | histogram | `peer` | Peer poll duration including retries. |
| `grid_peer_poll_retries_total` | counter | `peer`, `reason` | Retried peer poll attempts. |
| `grid_peer_poll_slow_total` | counter | `peer` | Peer polls exceeding the slow threshold. |
| `grid_peer_polls_in_flight` | gauge | | Peer polls currently in flight. |
| `grid_peer_response_bytes_total` | counter | `peer` | Bytes read from peer signal endpoints. |
| `grid_collection_up` | gauge | `peer` | Whether the last poll of this peer succeeded. |
| `grid_peer_last_success_timestamp_seconds` | gauge | `peer` | Unix time of the last successful poll of this peer. |

## Operator: serving signals to peers

The other half of the same path, where this site answers a peer's poll.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_peer_signals_refused_total` | counter | `peer`, `reason` | Peer observations refused at ingest. |
| `grid_signals_connections_shed_total` | counter | `limit` | Signals connections shed at accept. |

## Operator: reconciliation and probing

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_site_phase` | gauge | `site`, `phase` | GridSite phase: 1 for the current phase, 0 for the others. |
| `grid_site_phase_transition_total` | counter | `from_phase`, `to_phase`, `reason` | GridSite phase transitions. |
| `grid_agent_tool_provider_phase_transition_total` | counter | `from_phase`, `to_phase`, `reason` | AgentToolProvider phase transitions. |
| `grid_gateway_probe_total` | counter | `outcome`, `tls_mode` | Total gateway probe attempts. |
| `grid_gateway_probe_duration_seconds` | histogram | | Gateway probe duration. |
| `grid_mcp_probe_total` | counter | `outcome` | Total AgentToolProvider MCP tools/list probe attempts. |
| `grid_mcp_probe_duration_seconds` | histogram | | AgentToolProvider MCP tools/list probe duration. |

## Operator: identity and gossip

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_site_identity_expiry_timestamp_seconds` | gauge | | When the site identity certificate expires. |
| `grid_site_identity_rotations_total` | counter | `result` | Site identity rotation attempts. |
| `grid_swim_key_pending` | gauge | | 1 while SWIM holds traffic for its key. |
| `grid_swim_key_pending_dropped_total` | counter | | Inbound SWIM packets dropped while the key is pending. |

Certificate expiry is the one to alert on. Renewal starts when a third of the
lifetime remains, so this falling toward now means renewal has been failing for
a while. See [Site Identity](installation/enrollment.md).

## Gateway

One scrape per gateway replica, and each replica answers from its own learned
state, so two replicas agree only in steady state.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_route_decisions_total` | counter | | Site-selection decisions made. |
| `grid_route_selections_total` | counter | `path` | Decisions by the path that produced them. |
| `grid_route_site_rho` | gauge | site | Saturation the gateway last read for the site, in-flight over its ceiling. |
| `grid_route_site_weight` | gauge | site | Capacity the draw weights the site by. |
| `grid_route_site_ceiling` | gauge | site | Ceiling the gateway has learned for the site. |
| `grid_route_site_score` | gauge | site | Score the site was ranked by in the last decision. |
| `grid_route_shedding` | gauge | `model` | 1 while the gateway sheds this model, 0 otherwise. |
| `grid_route_prefix_affinity_total` | counter | `outcome` | Prefix-affinity decisions by outcome. |
| `grid_serving_config_reload_total` | counter | `result` | Serving-config reloads by result. |

A site gauge reads `NaN` once the gateway stops holding a value for that site,
which is how an unmeasured site is distinguished from one measured at zero. An
unmeasured site leaves the draw rather than ranking as idle.
