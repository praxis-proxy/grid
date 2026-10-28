# Prefix Affinity and Stored-State Pinning

The grid gateway's `grid_site_route` filter keeps a conversation on the site that already
holds its prompt in the prefix cache, while that site's queue allows. It also sends a
request that names a stored response or conversation to the site that stored it. This
follows the prefix-cache-affinity filter in llm-d-router, applied to sites.

## Prefix affinity

**What it keys on.** The gateway reads the request body for Chat Completions,
Completions, Anthropic Messages, and Responses. It hashes the prompt in 256-byte blocks:
tool definitions with their keys sorted, each message's role and text, and tool call names
and arguments. Each image, audio, video, or file part counts as one fixed digest, by
vLLM's `uuid` when the part has one. It ignores reasoning, generation parameters, and field
order. A tool list, tool call arguments, or other JSON value over 16 KiB keys by a digest
of its bytes as sent rather than in canonical form. The gateway reads arrays one element at
a time and stops once it has every key, so keying a body takes about its own size in memory
at most. A `cache_salt` gives its own keys, and so does each model. A body it cannot read,
token arrays, or a part type it does not know gets no keys and routes on load alone. The
gateway does not tokenize.

**What it remembers.** Each gateway replica keeps, in memory, which backend cluster it sent
each block to. A block counts toward a match only once the site answers with success, and
the gateway drops it when the site answers 429 or 5xx. A burst behind a new prompt
spreads by load until the first answer.

**How it chooses.** Selection orders the admitted candidates for a model by their windowed
worst queue depth and takes the first. When at least two are admitted, affinity first
narrows them to the sticky sites. A site is sticky when it holds the longest run of the
request's blocks. That run must cover 80 percent of the prompt or 32 blocks, about 8 KiB.
A run under 4 blocks never counts, so identical short prompts spread by load. Selection
then takes the least-queued sticky site. A site that admits no new request is never a
candidate, so it is never sticky.

**When load wins.** The sticky set reopens when the best sticky site's queue is deeper than
the best other site's by more than the match is worth. The match is worth the prefill it
saves, priced at `prefill_tokens_per_second`, then turned into queued requests at
`queued_request_seconds` each. A 40-block match saves about 2,560 tokens, or 0.26s at the
defaults, which is worth 0.13 of a queued request. A 400-block match is worth 1.3. An
unmeasured sticky site gives way to a measured one. Two percent of requests skip affinity,
so a second site learns a shared prompt.

## Stored responses and conversations

Response and conversation ids leave the gateway tagged with the site and backend cluster
that stored them, as `resp_<site>.<cluster>.<mac>.<id>` and `conv_<site>.<cluster>.<mac>.<id>`.
The cluster is an 8-digit hex digest of its name. A request naming a tagged id goes only to
that cluster, with the tag stripped, and is not hashed. A cluster that left the grid answers
404. One still in the grid that admits no new request answers 503 with `Retry-After: 5`. A
new conversation stays on the local site when it admits requests. Pinning is always on.

With `tag_key_path` set, the mac is a 64-bit HMAC-SHA256 of the site and cluster under that
key, and a tag without a valid mac answers 404, so a client cannot aim a request at a site
or cluster it was never given. Without a key the mac is empty and any well-formed tag is
accepted. A pinned request still passes admission, so it reaches only a cluster that could
serve it anyway. Set a key in production. The gateway replicas of one site share the key
through a mounted Secret, and a tagged id works only through the gateways holding it.

Site names go into these ids, so the gateway refuses a serving config whose site names are
not DNS-1123 labels.

## Settings

These live under `prefix_affinity` in the `grid_site_route` plugin configuration.
Helm exposes them through `gridServing.siteRoute.prefixAffinity`. The block is
optional, and every field has a default.

Default cache settings:

```yaml
prefix_affinity:
  capacity: 16384
  ttl_secs: 600
```

| Setting | Default | Effect |
|---|---|---|
| `enabled` | true | The off switch. Off routes on queue depth alone and reads no prompt. |
| `threshold` | 0.8 | Share of the prompt a site's run must cover to be sticky when it is under 32 blocks. Lower keeps more short conversations sticky. |
| `exploration` | 0.02 | Share of requests that skip affinity. Higher spreads a shared prompt sooner and costs more cache misses. |
| `capacity` | 16384 | Maximum entries in each backend cluster's LRU, per gateway replica. Must be a positive integer. See [Memory sizing](#memory-sizing). |
| `ttl_secs` | 600 | Lifetime after a successful response confirms an entry, in seconds. Must be a positive integer. |
| `prefill_tokens_per_second` | 10000 | Prices the prefill a match saves. Lower makes a match worth more queue. |
| `queued_request_seconds` | 2 | Seconds one queued request adds to a new request's wait. Lower lets a sticky site queue deeper. |
| `tag_key_path` | none | A file holding at least 32 bytes that authenticate stored-state tags. Set it in production. A changed file re-applies the config. |

Each backend cluster has its own LRU, so traffic to one cluster cannot evict
another cluster's entries. The replica's total entry capacity grows with the
number of backend clusters. The limit counts prefix entries, not bytes or
tokens. A plugin reconfiguration resizes existing LRUs, evicting the least
recent entries when shrinking. New clusters use the current limit. TTL changes
apply to subsequent confirmations; existing deadlines remain in place. The TTL
must fit the platform's monotonic clock. These mappings estimate locality;
successful response headers do not guarantee the engine still holds the KV cache.

The operator writes no `prefix_affinity` block, so the plugin defaults apply
unless configured separately. Blocks remain 256 bytes, with at most 512 keys
per request. A 32-block run is always sticky, and a run under 4 blocks never is.

## Memory sizing

`capacity` counts entries, not bytes. Each replica keeps one LRU per backend
cluster, and an expired entry stays in it until newer keys push it out, so
under steady traffic each LRU fills to `capacity` and stays full. Plan the
index at about `capacity × backend clusters × 128 bytes` per gateway replica.

An entry is a 48-byte LRU node plus its slots in the LRU's hash table. Measured
heap, before allocator overhead, is 82 bytes per entry when an LRU first fills
at a power-of-two capacity such as the default, and 116 bytes once eviction
churn has grown the hash table. Other capacities stay under 128 bytes. At the
default 16,384, one cluster takes about 1.3 MiB when its LRU first fills and
1.8 MiB once it churns.

| `capacity` | 1 cluster | 4 clusters | 16 clusters |
| --- | --- | --- | --- |
| 16,384 (default) | 2 MiB | 8 MiB | 32 MiB |
| 65,536 | 8 MiB | 32 MiB | 128 MiB |
| 262,144 | 32 MiB | 128 MiB | 512 MiB |

The table uses 128 bytes per entry and is per gateway replica.

**What an entry covers.** For a request's first 448 blocks (112 KiB), one entry
is one 256-byte block of distinct prompt text. Past that, the request's last 64
keys are spaced further apart, so a request takes at most 512 entries. The
default therefore holds the keys of at least 4 MiB of distinct prompt text per
cluster. An entry recorded for a request still waiting on its answer takes a
slot like a confirmed one.

**When capacity is too small.** The gateway exports no eviction or occupancy
metric. Watch the `sticky` share of `grid_route_prefix_affinity_total`. When it
falls and `no_match` rises while the replica count and the `not_applicable`
share hold steady, conversations are losing their keys before their next turn.
Raise `capacity`. As a rough check, compare the distinct prompt text each
cluster receives within `ttl_secs` with `capacity × 256` bytes.

**Memory limits.** The `praxis-gateway` chart sets no `resources` on the gateway
container by default, so nothing bounds the index unless the deployer sets a
memory limit. Size the request and limit from the gateway's observed peak plus
the index from the table above, then set them through the chart's `resources`
value. Every replica holds its own index.

```yaml
# praxis-gateway values. With 4 backend clusters at capacity 65536, the index
# adds about 32 MiB per replica.
resources:
  requests:
    memory: <observed peak + index>
  limits:
    memory: <observed peak + index + headroom>
```

## What to watch

`grid_route_prefix_affinity_total{outcome}` on the gateway's admin `/metrics`:

| Outcome | Meaning |
|---|---|
| `sticky` | Kept the request on the sites holding its prompt |
| `no_match` | No site held enough of it |
| `load_override` | The sticky site's queue outweighed the match |
| `exploration` | Skipped affinity |
| `not_applicable` | No readable prompt |

A request with one admitted site counts nothing.

| Symptom | Cause | Action |
|---|---|---|
| A conversation hops sites | More than one gateway replica. Each remembers only what it routed. | Keep-alive clients stay on one replica already. Set `sessionAffinity: ClientIP` on the gateway Service, or hash the front door on the API key. |
| A conversation hops sites on one replica | `load_override`: its site's queue outweighs the match | Expected under load. Check that site's queue depth. |
| A shared system prompt overloads one site | The first site that answered for the prompt holds it while its queue allows | Raise `exploration`, so another site learns the prompt sooner. |
| `sticky` stays low on a chat workload | Prompts under 4 blocks, unreadable bodies, or many replicas | Check `not_applicable` and `no_match`, and the replica count. |
| `sticky` falls and `no_match` rises as distinct prompts grow | `capacity` too small: keys leave the LRU before the next turn | Raise `capacity` and the gateway memory limit. See [Memory sizing](#memory-sizing). |
| A stored response or conversation returns 404 | Its site left the grid, and the state lives only there | Expected. The client starts a new response or conversation. |
| A stored response or conversation returns 503 | Its site is in the grid but admits no new request | Retry after the `Retry-After`. |

## Limits

Prefix affinity remembers per gateway replica, in memory, and starts empty after a
restart. It matches on hashes of the request text, not on engine tokens, so two requests
that render to the same tokens through different templates or JSON escaping do not match.
It sees only what this gateway routed. The load gate reads queue depth, the signal
selection orders by. It does not see in-flight work that has not queued.

The prefix index is shared by every client of a replica. A client that sends a guessed
prompt and sees which site serves it learns whether someone sent that prompt recently.
A `cache_salt` gives a tenant its own keys. The gateway does not seed keys with a tenant
identity, since in MaaS mode it has none.

A tagged id carries no tenant. Anyone holding a stored response's id can read, cancel or
delete it at its site, as with the engine's own store, which keys responses by id alone.

A provider-role gateway forwards only its allowed exact paths, so a stored response held
at another site cannot be fetched by id through it.
