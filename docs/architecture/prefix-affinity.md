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
order. A `cache_salt` gives its own keys, and so does each model. A body it cannot read,
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

Response and conversation ids leave the gateway tagged with the site that stored them, as
`resp_<site>.<id>` and `conv_<site>.<id>`. A request naming one goes only to that site, with
the tag stripped, and is not hashed. A site that left the grid answers 404. A site still in
the grid that admits no new request answers 503 with `Retry-After: 5`. A new conversation
stays on the local site when it admits requests. Pinning has no setting and is always on.

Site names go into these ids, so the gateway refuses a serving config whose site names are
not DNS-1123 labels.

## Settings

These live under `prefix_affinity` in the grid serving config. The block is optional, and
every field has a default.

| Setting | Default | Effect |
|---|---|---|
| `enabled` | true | The off switch. Off routes on queue depth alone and reads no prompt. |
| `threshold` | 0.8 | Share of the prompt a site's run must cover to be sticky when it is under 32 blocks. Lower keeps more short conversations sticky. |
| `exploration` | 0.02 | Share of requests that skip affinity. Higher spreads a shared prompt sooner and costs more cache misses. |
| `prefill_tokens_per_second` | 10000 | Prices the prefill a match saves. Lower makes a match worth more queue. |
| `queued_request_seconds` | 2 | Seconds one queued request adds to a new request's wait. Lower lets a sticky site queue deeper. |

The operator writes no `prefix_affinity` block, so these defaults apply. These values do not
change: 256-byte blocks and at most 512 keys per request. A 32-block run is always sticky,
and a run under 4 blocks never is. Each cluster remembers 16,384 blocks. A block with no
answer yet lasts 30s, and a confirmed one 10 minutes.

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
| A stored response or conversation returns 404 | Its site left the grid, and the state lives only there | Expected. The client starts a new response or conversation. |
| A stored response or conversation returns 503 | Its site is in the grid but admits no new request | Retry after the `Retry-After`. |

## Limits

Prefix affinity remembers per gateway replica, in memory, and starts empty after a
restart. It matches on hashes of the request text, not on engine tokens, so two requests
that render to the same tokens through different templates or JSON escaping do not match.
It sees only what this gateway routed. The load gate reads queue depth, the signal
selection orders by. It does not see in-flight work that has not queued.
