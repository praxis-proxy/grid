# Polling Cross-Site Load Signals

A gateway routes across sites by preferring the least-loaded peer that serves a
model. To do that it needs each peer's current load. Each gateway polls its
peers' signal endpoints over mutual TLS, records the readings in a local store,
and the router orders cross-site candidates from that store.

This document covers the poll path and its failure behavior. The signal wire
format and the store are described in [Signals](signals.md). How the ordered
candidates are selected is in [Routing](routing.md) and [Scoring](scoring.md).

## The Poll Path

Polling is direct per peer, not through a relay. One poller dials one peer,
verifies that peer's Grid site certificate, and attributes the readings to that
one verified identity.

| Step | What happens |
|---|---|
| Dial | The poller opens a mutual-TLS connection to the peer's `/v1/site/signals`. Both ends present Grid site certificates. |
| Verify | The peer's certificate is checked against the Grid CA and its SPIFFE identity, then the verified identity is compared to the site the poller intended to reach. A valid Grid peer answering for a site the poller did not dial is refused. |
| Read | The exposition body is read under a byte ceiling and a time bound, so a slow or oversized peer cannot hold the poll open or exhaust memory. |
| Store | Each reading is keyed on the verified peer identity, never a value the response body carries. A body label that disagrees with the verified owner is dropped. |

The verified peer identity is the store key. The response body cannot choose
where its readings land, so one peer cannot inject readings attributed to
another.

## When a Gateway Polls

A gateway polls its peers only while its own endpoint is actively serving.

Cross-site routing is a decision this gateway makes when it receives a request
it can place elsewhere. A gateway that is not serving receives no such requests.
It has no routing decision to inform, so it holds no mutual-TLS connections to
its peers and adds no scrape load to them. Polling follows serving. It starts
when the gateway begins serving and stops when it drains.

The number of live peer connections is then proportional to the serving
gateways, not to the size of the grid. A drained or standby gateway is silent on
the peer signal endpoints.

## From Poll to Route

The poll path and the route path meet at the store, and only at the store.

The poller writes readings into the store as they arrive. Ordering runs off the
request path as a control step: it reads each candidate's recent worst load from
the store and produces a candidate list ordered least-loaded-first. The request
path reads one ordered snapshot and takes the front admitted candidate. The
request path does not read raw signals or compute load. It reads resolved order.

## Failure Behavior

| Condition | Signal produced | Routing effect |
|---|---|---|
| No reading for a candidate | The candidate scores as maximally loaded. | It sorts after every candidate that has a reading, so a measured healthy peer is preferred over an unmeasured one. |
| Readings all stale (older than the window) | Same as no reading. | The candidate sorts last until a fresh reading arrives. |
| Peer unreachable or slow | The poll returns an error and no reading is written. | The candidate ages out of the window and then sorts last. The poll loop continues, and one unreachable peer does not wedge the others. |
| Peer presents an untrusted or mismatched certificate | The connection is refused, so no reading is written. | The peer contributes nothing to the order. |

Loss of signal degrades to "least preferred," never to "silently treated as
idle." A drained burst stays penalized until it ages out of the window rather
than snapping back to idle on the first missing sample.

## See Also

- [Signals](signals.md), the signal wire format and the store.
- [Routing](routing.md), how an ordered candidate is selected.
- [Scoring](scoring.md), how load maps to order.
- [Authentication](auth.md), the Grid mTLS peer identity layer the poll path uses.
