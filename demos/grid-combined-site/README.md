# Grid Combined-Site Demo

This demo models three Kubernetes clusters named `west`, `central`, and
`east`. Every cluster is one Grid site and runs both sides of the inference
path as separate workloads:

- a Praxis consumer gateway called directly by local workloads;
- a Praxis provider gateway for the authenticated provider boundary;
- a private simulated inference endpoint downstream of the provider gateway;
- a Grid operator that discovers provider state and renders the local routing
  overlay.

There is no public entry point, global traffic manager, or separate edge
cluster.
The topology matches the compact combined-site installation documented under
`examples/helm/existing-clusters/` while remaining a disposable Kind demo.

See the architecture overview's
[Deployment Topologies](../../docs/architecture/overview.md#deployment-topologies)
section for the security, failure-domain, networking, operational, and
failover tradeoffs between combined sites and dedicated gateway clusters.

## Status

This directory currently defines the implementation contract and file
ownership for the standalone demo. The Forge configuration, manifests, Helm
values, xtask command, assertions, and runtime evidence must be implemented
and validated before the demo is presented as runnable.

## User Story

As a platform workload, I want to submit inference through the consumer
gateway in my local cluster and let Grid select an eligible local or remote
provider without traversing a public traffic-routing layer.

As a platform operator with a limited cluster budget, I want each cluster to
act as both a consumer site and a provider site while retaining separate
gateway identities, configuration, credentials, and authorization boundaries.

## Topology

```text
                         Grid membership and provider state
              +---------------------+---------------------+
              |                     |                     |
              v                     v                     v
+-------------------------+ +-------------------------+ +-------------------------+
| west cluster            | | central cluster         | | east cluster            |
|                         | |                         | |                         |
| workload                | | workload                | | workload                |
|    |                    | |    |                    | |    |                    |
|    v                    | |    v                    | |    v                    |
| consumer gateway        | | consumer gateway        | | consumer gateway        |
|    | Grid selection     | |    | Grid selection     | |    | Grid selection     |
|    +----------+         | |    +----------+         | |    +----------+         |
|               v         | |               v         | |               v         |
| provider gateway        | | provider gateway        | | provider gateway        |
|    |                    | |    |                    | |    |                    |
|    v                    | |    v                    | |    v                    |
| private inference       | | private inference       | | private inference       |
| endpoint                | | endpoint                | | endpoint                |
+-------------------------+ +-------------------------+ +-------------------------+
              ^                     ^                     ^
              |                     |                     |
              +----- eligible remote provider paths -----+
```

The diagram shows the preferred local path inside each cluster. Grid may
select a provider gateway in either of the other clusters when the local
provider is ineligible or draining. A private inference endpoint is always
downstream of its provider gateway; consumers never route directly to the
backend.

Colocation does not merge the consumer and provider roles. They remain
separate Deployments and Services with separate Praxis configuration, TLS
identity, ServiceAccount, and Secret mounts. Provider credentials are mounted
only in provider gateways.

## Planned Layout

```text
demos/grid-combined-site/
  README.md
  forge.yaml
  configs/
    README.md
    consumer/
    provider/
  resources/
    README.md
    common/
    west/
    central/
    east/
  values/
    README.md
    west/
    central/
    east/
```

The demo owns its `forge.yaml` and must not modify the GLB demo configuration
at runtime. Reusable generation code may be shared in Rust, but the rendered
topology and its inputs must be inspectable from this directory.

## Request Paths

The demo must prove these paths:

1. A workload in each cluster sends a request to its local consumer gateway.
2. Each consumer gateway initially selects its local provider gateway.
3. Each provider gateway authenticates the consumer peer and authorizes the
   exact candidate, model, path, and backend cluster.
4. The provider gateway replaces the provider credential only at the final
   hop and sends the request to its private inference endpoint.
5. Draining one local provider causes new requests at that site to use an
   eligible remote provider.
6. Restoring the provider returns new requests to the local path without
   restarting a consumer gateway.

## Optional External Provider

The demo should reuse the generic external-provider flags rather than add an
OpenAI-specific command:

```bash
cargo xtask env run-grid-combined-site-demo \
  --external-provider openai \
  --external-provider-site central \
  --external-provider-key-file /path/to/openai-key \
  --external-provider-model gpt-4o-mini \
  --quick \
  --teardown
```

See [e2e-demo-output.txt](e2e-demo-output.txt) for example narrated output
from a quick cold run with external OpenAI provider.

Without these flags, the demo must create no external-provider Secret,
configuration, candidate, or evidence. With the flags, only the selected
site's provider gateway may mount the credential. The generated upstream
cluster must configure HTTP authority and TLS SNI independently. Evidence
must identify the selected consumer site, provider site, candidate, model,
and serving overlay revision without recording credential material or model
response content.

The provider enum and descriptor should remain extensible so another external
provider can be added without changing the combined-site orchestration
contract.

## Required Proof

Quick mode should prove:

- three Kind clusters become healthy;
- all three Grid sites join the same membership view;
- each site receives a versioned routing overlay;
- each workload reaches its local consumer gateway;
- all three local simulated providers serve successful requests;
- provider-gateway mTLS, peer authorization, credential replacement, and
  backend NetworkPolicy remain enforced;
- external-provider resources are absent when the feature is disabled;
- teardown removes all demo-owned clusters and networks.

Full mode should additionally prove:

- local-provider preference at west, central, and east;
- remote-provider fallback after one local provider is drained;
- existing-session behavior during drain where supported by the routing
  contract;
- guaranteed provider restoration even when a fallback assertion fails;
- routing returns to the restored local provider after overlay hot reload;
- sequential Grid operator restart recovery;
- sustained inference after recovery;
- optional live OpenAI inference when explicitly enabled.

Every run must write human-readable narration and a machine-readable
`results.json` containing the mode, topology, image references, proof results,
and exact overlay revisions.

## Non-Goals

- Public traffic entry, GTM emulation, or DNS failover.
- Regional affinity or enforced region locking.
- Collapsing consumer and provider gateways into one process.
- Sharing provider credentials with consumer gateways or Grid operators.
- Production scalability or failure-domain claims based on Kind.
- Provider API translation beyond the currently supported OpenAI-compatible
  request path.

## Relationship To Other Demos

- `grid-glb-demo` proves external client traffic through a global traffic
  manager and logical edge gateways.
- `grid-workload-inference` proves separated consumer and provider roles
  across four clusters.
- `grid-combined-site` proves the compact three-cluster form where every site
  contains both roles while retaining the provider security boundary.
