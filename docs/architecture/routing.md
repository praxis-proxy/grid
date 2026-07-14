# Routing

Grid routing is split between the Grid Operator control plane and the Praxis
data plane. The operator renders routing state. Praxis consumes that state and
proxies requests.

## Overview

```text
GridNetwork + InferenceProvider CRDs
  + provider metrics
  + CRDT provider records
        |
        v
Grid Operator
        |
        v
Routing overlay ConfigMap
        |
        v
Praxis consumer gateway
        |
        v
Praxis provider gateway
        |
        v
llm-d / EPP / inference backend
```

Grid does not proxy traffic. It writes the overlay used by Praxis filters.

## Control-plane rendering path

For each `GridNetwork` and gateway reference, the operator:

1. Lists local `InferenceProvider` resources for the network.
2. Collects provider metrics from `spec.metricsConfig` when configured.
3. Reads remote provider records received through CRDT state.
4. Converts providers into scoring backends and routing candidates.
5. Scores and orders candidates.
6. Server-side applies an overlay `ConfigMap`.

The overlay key is `grid-config.json`.

## Routing overlay format

The overlay is a compact JSON document consumed by Praxis:

```json
{
  "network": "production",
  "local_site": "site-east",
  "candidates": [
    {
      "kind": "inference_model",
      "name": "model-east",
      "site": "site-east",
      "cluster": "gateway-site-east",
      "fresh": true
    },
    {
      "kind": "inference_model",
      "name": "model-west",
      "site": "site-west",
      "cluster": "gateway-site-west",
      "fresh": true
    }
  ]
}
```

Candidate fields:

| Field | Meaning |
|-------|---------|
| `kind` | Capability kind, currently `inference_model` for model routing. |
| `name` | Model or capability name matched by the consumer gateway. |
| `site` | Grid site advertising the capability. |
| `cluster` | Praxis load-balancer cluster identity used for upstream routing. |
| `fresh` | Whether provider status is considered fresh enough for normal routing. |

## Candidate scoring and ordering

The operator orders candidates before writing the overlay. It uses
`scoring::score_backends` with provider configuration, optional live metrics,
and optional CRDT-propagated provider metrics.

`Unavailable` providers are excluded. `Degraded` providers remain in the
overlay with `fresh: false`. Providers with no live metrics use neutral metric
scores.

At request time, `grid_route` selects from this pre-sorted candidate list rather
than recomputing the full scoring formula.

## Consumer gateway selection

The Praxis consumer gateway extracts request facts such as the requested model
and runs `grid_route` against the overlay. For model inference, the filter scans
for matching `inference_model` candidates and sets the selected Praxis upstream
cluster.

If no candidate matches, the request fails cleanly instead of falling through to
an unrelated backend.

## Provider gateway trust

Provider gateways terminate mTLS and run `grid_ingress_trust` before forwarding
traffic to local inference infrastructure. The filter checks the verified peer
identity from the downstream client certificate and rejects untrusted peers with
HTTP 403.

The current development trust policy matches the peer certificate organization.
Production policies should prefer stronger identity binding such as certificate
digest pinning or SPIFFE-style identities.

## Provider-side handoff

After site selection, provider-side Praxis filters hand the request to local
inference infrastructure. A common llm-d-style path is:

```text
provider gateway
  -> ext_proc
  -> llm-d external processor
  -> endpoint_selector
  -> inference pod or service
```

Grid chooses the provider site. llm-d or the provider-local scheduler chooses
the concrete pod, GPU, or endpoint inside that site.

## Metrics and CRDT inputs

Local provider metrics enter routing through `InferenceProvider.spec.metricsConfig`.
Remote provider records enter routing through CRDT state broadcast over SWIM.
Both inputs are converted into the same scoring model before overlay rendering.

Remote records are filtered by network and local site identity so a site does
not route to its own CRDT echo or to providers from another `GridNetwork`.

## Relevant files

| File | Role |
|------|------|
| `operator/src/controller/grid_network.rs` | Reconcile loop wiring for metrics, CRDT snapshots, overlay rendering, and status. |
| `operator/src/resources/routing_overlay.rs` | Provider-to-candidate mapping, scoring input construction, and overlay JSON rendering. |
| `operator/src/resources/provider_metrics.rs` | Prometheus scrape and metric-name mapping for `metricsConfig`. |
| `scoring/src/scoring.rs` | Six-signal backend scoring implementation. |
| `swim/src/state_broadcast.rs` | CRDT state broadcast handler used by SWIM custom broadcasts. |
| `xtask/src/env/consumer.rs` | Local validation consumer gateway configuration. |
| `xtask/src/env/gateway.rs` | Local validation provider gateway configuration. |
| `xtask/src/env/operator.rs` | Local validation fixtures and overlay checks. |
