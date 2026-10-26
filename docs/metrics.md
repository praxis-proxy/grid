# Metrics Reference

Every metric the grid exports, what it means, and where to scrape it. Three
components expose Prometheus endpoints and they answer different questions.

| Endpoint | Component | Serves |
|---|---|---|
| `/metrics` on `GRID_METRICS_ADDR`, default `0.0.0.0:9090` | Site operator | Reconciliation, probing, peer polling, and the provider readings this site publishes |
| `/metrics` on the gateway metrics listener, `metricsListener.enabled` in the gateway chart, port 9443, TLS only | Grid gateway | Site selection, shedding, serving-config reloads, and its poll of the local operator |
| `/metrics` on `OVERLAY_SYNC_HEALTH_ADDR`, default `0.0.0.0:9091` | Overlay-sync sidecar, when `overlay.sidecar.enabled` | Delivery of the serving overlay from its ConfigMap to the gateway |

The `grid-gateway` operand built with Praxis 0.7.4 serves `/metrics`, `/healthy`,
and `/ready` on `admin.metrics_address`. When upgrading from Praxis 0.7.3,
move the probe listener from `admin.address` to `admin.metrics_address`:

```yaml
admin:
  metrics_address: "127.0.0.1:9901"
```

Keep the listener address consistent with the deployment's probe commands.
`admin.address` now serves administration endpoints; configure a different
port if those are also needed. Chart defaults and operator-rendered configs
must be qualified against the selected image before rolling out the new
operand. The chart's separately released Praxis AI image has its own version
and upgrade requirements.

The operator exports each provider's resolved readings, not the raw engine
series behind them. The series a gateway routes on travel over
`/v1/site/signals`, described in [Signals](architecture/signals.md). A series
the operator holds but does not name below never reaches `/metrics`.

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

## Operator: serving signals

The other half of the same path, where this site answers a peer's poll or
relays every site's readings to its own gateway.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_peer_signals_refused_total` | counter | `peer`, `reason` | Peer observations refused at ingest. |
| `grid_signals_connections_shed_total` | counter | `limit` | Signals connections shed at accept. |
| `grid_signals_relay_truncated_total` | counter | `store`, `cut` | Targets dropped and lines cut from the relay served to the local gateway. |

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
| `grid_operator_lease_held` | gauge | | 1 while this replica holds the operator lease. At most one replica reports 1, and none while no replica holds it. |
| `grid_operator_routed` | gauge | | 1 while the signals and SWIM Services route to this replica. It drops before the lease is released. |
| `grid_operator_lease_waiting` | gauge | `reason` | 1 for why this replica is not contending for the lease: `unelected_replica` while a replica of the operator Deployment runs without leader election, `replicas_unreadable` while the replicas cannot be listed. |
| `grid_swim_key_pending_dropped_total` | counter | | Inbound SWIM packets dropped while the key is pending. |

Certificate expiry is the one to alert on. Renewal starts when a third of the
lifetime remains, so this falling toward now means renewal has been failing for
a while. See [Site Identity](installation/enrollment.md).

## Gateway

One scrape per gateway replica, and each replica answers from its own learned
state, so two replicas agree only in steady state.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_route_decisions_total` | counter | `site`, `cluster`, `reason` | Requests the gateway decided, by outcome. A refusal records an empty site and cluster. |
| `grid_route_selections_total` | counter | `path` | Decisions by the path that produced them. |
| `grid_route_site_rho` | gauge | `site`, `cluster` | Smoothed saturation used by the draw: the nearer of the reported and counted limits. |
| `grid_route_site_rho_reported` | gauge | `site`, `cluster` | KV-cache mean plus spread, joined per scrape; `NaN` when absent. |
| `grid_route_site_rho_counted` | gauge | `site`, `cluster` | In-flight over the learned ceiling; `NaN` until a hard bound makes it a reading. |
| `grid_route_site_weight` | gauge | `site`, `cluster` | Capacity the draw weights the site by. |
| `grid_route_site_ceiling` | gauge | `site`, `cluster` | Lesser of the learned soft and hard bounds, never below the floor. |
| `grid_route_site_ceiling_soft` | gauge | `site`, `cluster` | Most requests the current site size carried with nothing waiting; `NaN` until observed. |
| `grid_route_site_ceiling_hard` | gauge | `site`, `cluster` | Fewest requests held at a demonstrated limit; `NaN` until observed. |
| `grid_route_site_in_flight` | gauge | `site`, `cluster` | Requests held in the reading used by the decision. |
| `grid_route_site_full` | gauge | `site`, `cluster` | 1 after a backlog at the ceiling spans `full_after_ms`, 0 otherwise, `NaN` without a queue signal. |
| `grid_route_site_reading_age_seconds` | gauge | `site`, `cluster` | Age of the newest sample used by the decision. |
| `grid_route_site_silent` | gauge | `site`, `cluster` | 1 while a site that has published has sent nothing within the freshness window, until it sends two new samples; it takes no request while a live site can. 0 otherwise. |
| `grid_route_site_silent_transitions_total` | counter | `site`, `cluster`, `to` | Sites entering (`silent`) or leaving (`live`) silence. A steady rate is a flapping link. |
| `grid_route_site_last_sample_age_seconds` | gauge | `site`, `cluster` | Seconds since the site's newest sample. Unlike `grid_route_site_reading_age_seconds`, which goes `NaN` once the sample leaves the freshness window, this keeps counting. |
| `grid_route_site_round_age_seconds` | gauge | `site`, `cluster` | Seconds since the operator's last poll round that tried this peer; a peer's silence is judged at that round. `NaN` for this site or when the operator reports no rounds. A growing value with the poller running is a stalled poller, under which no peer goes silent. |
| `grid_route_site_score` | gauge | `site`, `cluster` | Score the site ranks by in the current snapshot. |
| `grid_route_shedding` | gauge | `model` | 1 while the gateway sheds this model, 0 otherwise. |
| `grid_route_prefix_affinity_total` | counter | `outcome` | Prefix-affinity decisions by outcome. |
| `grid_serving_config_reload_total` | counter | `result` | Serving-config reloads by result. |

A `NaN` rho or weight means the gateway holds no reading for that site, which
distinguishes an unmeasured site from one measured at zero. A `NaN` score means
the candidate is excluded or demoted from the order. A site that leaves the
topology reads `NaN` on all four, since the exporter keeps a series until
restart.

## Gateway: polling the local operator

Every site's load reaches the gateway through one poll of its local operator,
described in [Site Selection](site-selection.md).

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_signals_poll_total` | counter | `result` | Polls of the local operator by result. |
| `grid_signals_last_success_timestamp_seconds` | gauge | | Unix time of the last successful poll. |
| `grid_signals_response_bytes` | gauge | | Size of the last successful poll's response. |
| `grid_signals_ingest_dropped_total` | counter | `reason` | Rows the operator served that the gateway refused, by reason. |

## Overlay-sync

The sidecar that delivers the serving overlay to the gateway, described in
[Routing](architecture/routing.md). One scrape per gateway pod.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `grid_overlay_sync_ready` | gauge | | 1 once the sidecar holds a valid overlay, written or restored from the shared volume. |
| `grid_overlay_sync_degraded` | gauge | | 1 while the sidecar serves its last good overlay because the API is unreachable, the ConfigMap was deleted, or its key is missing. |
| `grid_overlay_sync_events_total` | counter | `outcome`, `reason` | Overlay events by outcome and reason. |
| `grid_overlay_sync_file_writes_total` | counter | `outcome` | Overlay file writes by outcome. |
| `grid_overlay_sync_validation_failures_total` | counter | `reason` | Overlays refused at validation, by reason. |
| `grid_overlay_sync_watch_reconnects_total` | counter | `reason` | ConfigMap watch reconnections by reason. |
| `grid_overlay_sync_last_observed_timestamp_seconds` | gauge | | Unix time of the last observed ConfigMap event. |
| `grid_overlay_sync_last_write_timestamp_seconds` | gauge | | Unix time of the last successful overlay write. |
| `grid_overlay_sync_payload_bytes` | gauge | | Size of the last written overlay. |
