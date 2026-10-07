# Gateway examples

Two files the grid gateway reads, with a different author each.

## grid-site-route.yaml

The `grid_site_route` filter block in the gateway's praxis config, with the `load_balancer`
block that always follows it: the filter sets the cluster, the load balancer dials it and runs
the health checks selection demotes on. A user sets it through the chart's
`gridServing.siteRoute` values (`availability` and `prefixAffinity`, rendered as
`availability` and `prefix_affinity`). Nothing needs setting: every field has a default and
the block may be empty. The example sets the one switch, `shedding`. A change is a chart value
and a rollout.

`availability` governs how a site's availability is measured from its in-flight count and
what the gateway does with it. The fields other than `shedding` are for a symptom in
`docs/site-selection.md`, not for a first deployment.

| Field | Default | Meaning |
|---|---|---|
| `smoothing` | 0.3 | How far one new sample moves a site's saturation toward the new reading. |
| `ceiling_half_life_ms` | 600000 | How long a learned ceiling takes to halve once load falls away. |
| `ceiling_floor` | 8 | The least a ceiling can be, so a quiet site does not read as full. |
| `explore_floor` | 0.25 | A measured site's weight is at least this share of the largest ceiling. |
| `full_after_ms` | 5000 | How long a site must stay at its ceiling with work waiting before it counts as full. |
| `room_after_ms` | 2000 | How long no sample may show a site at its ceiling with work waiting before a shedding model routes again. |
| `queue_full` | 1.0 | Queued work per serving unit at which a site counts as full. Teaching stops on any whole request waiting. |
| `shedding` | false | Whether a model whose every site is full answers 429 with Retry-After. Keep it off where the EPP's flow control is on: two layers refusing on their own signals shed twice for one overload, and the grid's 429 would arrive while the EPP is still holding the request. |

`prefix_affinity` governs how strongly a conversation keeps to the site holding its prompt.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | true | Prefer the site holding a request's prompt. Off routes on load alone. |
| `threshold` | 0.8 | Share of the request's keys a site must hold to be sticky. |
| `exploration` | 0.02 | Share of requests that skip affinity, so a second site learns a shared prefix. |
| `prefill_tokens_per_second` | 10000 | Prompt tokens a site prefills per second, pricing the cache a match saves. |
| `queued_request_seconds` | 2.0 | Seconds one queued request adds to a wait, turning that price into queue depth. |
| `tag_key_path` | none | A file holding the key that authenticates stored-state tags. |

Tuning order, one metric against one truth per step: `grid_route_site_ceiling` against the
engine's running plateau, `grid_route_site_rho` against the engine queue,
`grid_route_selections_total{path}` mostly `two_choices`, and with shedding on
`grid_route_shedding{model}` against the 429 rate. `docs/routing.md` section 7b has the
procedure and what to move at each step.

## serving-config.json

The serving config the operator writes for each gateway and the gateway re-reads every few
seconds. A user does not edit it. It is here because the gateway and the operator share it as
a golden test, so the example is the contract between them.

| Field | Meaning |
|---|---|
| `local_site` | This gateway's own site. |
| `window_secs` | How long the store keeps each series. |
| `load_window_ms` | The freshness window selection reads, derived from the operators' scrape interval. |
| `candidates[]` | One routable model per site: `kind` (always `inference_model`), `name`, `site`, `cluster` (the `load_balancer` cluster and the label its load is keyed on), `fresh`, and `admission` when the site takes no new requests. |
| `peers[]` | One operator signals endpoint per site, sorted by site: `site`, `addr`, `server_name` and `authority` for mutual TLS, `path`, `interval_ms` (500 for the local operator, 5000 for peers), `connect_timeout_ms`, `request_timeout_ms`, the three identity paths, `pins` under pin trust, and `gateway` when the site is dialed through its own gateway rather than a cluster. |
