# Scaling the signals listener

Each site keeps one pooled connection to each peer and polls it every 5 seconds. The listener closes a connection after 5 minutes and the peer reconnects, so a grid of N sites puts about N - 1 connections on each listener. An idle connection costs some tens of kilobytes and one file descriptor.

## Bounds

| Bound | Default |
|---|---|
| Total connections | members x 8 + 72, from 256 to 4096 |
| Connections per peer site | 8 (`GRID_SIGNALS_MAX_PER_PEER`) |
| Handshakes in progress | 64, refusing the newest past that |
| Handshakes from one source address | 8, or 32 behind a Route (`signals.maxHandshakesPerSource`) |
| Handshakes from one source that have sent nothing | 8, a quarter of the share |
| Silence before a connection is dropped | 500 ms |

Catalogs are read again only when a peer's version changes, and status is written only when a verdict changes, after 3 missed rounds.

## Behind an OpenShift router

Through a passthrough Route the operator sees every peer at the router's address, and the router applies its per-client limits to each peer's egress address. Sites behind one egress address share it, so the defaults of 10 concurrent connections and 20 a second fit about 5 to 10 such sites.

The chart raises the per-source cap to 32, the whole handshake share, whenever it renders a
signals Route. That is deliberate: with every peer at one address, a cap sized for one peer
sheds the rest. An install that sets `signals.maxHandshakesPerSource` keeps its own value,
and without a Route the default stays 8.

```sh
helm upgrade grid-operator charts/grid-operator --reuse-values \
  --set signals.maxHandshakesPerSource=48
```

Two limits cover one source and they are separate. The cap above counts handshakes in
progress and is loose behind a router by design. A second cap counts only connections from
that source which have sent nothing yet, at 8, a quarter of the share. The listener drops a
connection that sends nothing for 500 ms. A peer set behind one router therefore gets the
whole share between them, while one source cannot hold the share by opening connections and
staying quiet. Raising `signals.maxHandshakesPerSource` does not raise the quiet cap.

Size the router limits for the peers sharing one egress address, and allow only the peers' egress ranges:

```sh
oc -n <operator-namespace> annotate route <signals-route> --overwrite \
  haproxy.router.openshift.io/rate-limit-connections=true \
  haproxy.router.openshift.io/rate-limit-connections.concurrent-tcp=40 \
  haproxy.router.openshift.io/rate-limit-connections.rate-tcp=80 \
  haproxy.router.openshift.io/ip_allowlist="203.0.113.0/24 198.51.100.0/24"
```

Or skip the router, so the operator sees each peer's own address:

```sh
helm upgrade grid-operator charts/grid-operator --reuse-values \
  --set signals.service.type=LoadBalancer \
  --set signals.service.externalTrafficPolicy=Local \
  --set 'signals.service.loadBalancerSourceRanges={203.0.113.0/24,198.51.100.0/24}'
```

A distinct egress address per site (an OVN-Kubernetes EgressIP on the operator namespace) or a dedicated router shard for grid Routes also removes the shared budget.

Check on the target cluster that the router enforces these annotations on a passthrough Route before relying on them.
