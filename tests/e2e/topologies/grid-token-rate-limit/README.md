# Grid-aware token-rate-limit Stage 2 topology

This Forge topology is the distributed quota proof for the token-rate-limit
POC. It is separate from the generic provider-traffic and llm-d demonstrations.

The west cluster runs two independently addressable Praxis consumer deployments:
`consumer-gateway-a` and `consumer-gateway-b`. Both use the same authenticated,
cluster-private Valkey service and the same logical quota namespace, rule, and
canonical model. Central and east remain attributable provider sites with their
own VCR-backed provider gateways.

The routing contract is Grid selection groups with `noMetrics` scoring and
`selection_policy.mode: roundRobin`. No llm-d, EPP, pressure generator, queue
metric, or KV-cache metric is part of this topology.

Valkey owns shared quota state only. Provider round-robin counters remain local
to each consumer; this topology does not claim a globally synchronized provider
sequence, cross-cluster Valkey reachability, or Valkey high availability.

The Valkey password and connection URL are delivered through the `valkey-auth`
Kubernetes Secret. Consumers receive the URL through a Secret-backed environment
variable; the Praxis configuration supports the exact `${ENV_VAR}` form for this
backend URL. The service is ClusterIP-only and its NetworkPolicy permits access
only from the two quota-client consumer pods.

Before distributed measurement, the harness must wait for both consumers to
serve the same overlay revision and for the asynchronous Valkey reconciliation
worker completion metric to advance. Consumer restarts are not quota resets in
this topology.
