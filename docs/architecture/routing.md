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

## Backend kinds

`InferenceProvider.spec.backendKind` is a placement and policy category. It is
not strictly a wire-protocol choice, and it does not by itself mean a route does
or does not use a Praxis gateway.

| `backendKind` | Meaning | Typical path | Placement intent |
|----------------|---------|--------------|------------------|
| `local` | Self-hosted capacity in the local site. | Consumer Praxis directly to local/provider-side Praxis or local backend cluster. | Prefer first when healthy. |
| `remote` | Self-hosted capacity in another Grid site. | Gateway-to-gateway mTLS to a remote Praxis provider gateway. | Prefer after local Grid-owned capacity. |
| `cloud_managed` | Managed model capacity under the operator's cloud account. | Praxis gateway, provider adapter, or direct managed-service endpoint depending on deployment. | Prefer after Grid-owned capacity and before generic SaaS fallback. |
| `api_provider` | Third-party API/SaaS provider fallback. | Praxis injects configured provider credential and forwards to the API endpoint. | Last-resort or explicit fallback tier. |

`cloud_managed` is distinct because Grid should apply different cost,
credential, observability, and placement assumptions than it applies to
self-hosted sites. A deployment may still place Praxis in front of a
cloud-managed backend; the category describes the operational boundary, not a
requirement to bypass Praxis.

## Multi-cluster model routing

Multi-cluster model routing is the baseline Grid data-plane behavior:

1. Each provider site declares the models it can serve through
   `InferenceProvider.spec.models`.
2. `spec.routingClusterRef` names the Praxis upstream cluster that can reach
   that provider site.
3. The operator renders one overlay candidate per routable model/provider pair.
4. The consumer Praxis gateway extracts the requested model and selects the
   first matching candidate from the ordered overlay.
5. For remote sites, traffic goes gateway-to-gateway over mTLS before reaching
   provider-local filters and serving infrastructure.

Example overlay shape:

```json
{
  "kind": "inference_model",
  "name": "model-west",
  "site": "site-west",
  "cluster": "gateway-site-west",
  "fresh": true
}
```

In that example, a request for `model-west` selects the `gateway-site-west`
Praxis cluster. The concrete pod or endpoint inside `site-west` is still chosen
by the provider-side serving stack, such as llm-d/EPP endpoint selection.

## API-provider fallback

API-provider fallback uses the same overlay mechanism as self-hosted routing.
The difference is the backend category and credential boundary:

1. An `InferenceProvider` declares `backendKind: api_provider`.
2. The operator includes the API provider as a candidate when it is available.
3. Scoring normally places self-hosted candidates ahead of API-provider
   candidates, so API providers are used as fallback or explicit lower-priority
   routes.
4. Praxis applies configured credential injection before forwarding the request
   to the provider endpoint.
5. If no self-hosted candidate is available for a model, the API-provider
   candidate can become the selected route.

The fallback decision is therefore still local to the consumer gateway at
request time: `grid_route` selects from the pre-rendered candidate list, and the
Praxis filter chain handles credential injection and upstream forwarding.

Current local validation uses mock API-provider endpoints. That proves the Grid
overlay and Praxis routing/credential-injection mechanics. It does not prove a
real external provider protocol such as OpenAI, Anthropic, Bedrock SigV4, or
Vertex OAuth2.

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
