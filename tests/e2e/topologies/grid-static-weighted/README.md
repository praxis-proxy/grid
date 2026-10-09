# Static weighted provider qualification

This isolated three-site Forge topology qualifies operator-configured relative
provider capacity without changing the ordinary `grid-provider-traffic`
round-robin qualification. Static weighting is not queue-depth, KV-cache, or
other live-metric routing: it has no scraping, normalization, smoothing,
hysteresis, availability floor, or recovery timer.

```mermaid
flowchart LR
  C[Persistent client] --> G[Praxis consumer gateway]
  G --> O[Grid weighted overlay]
  O --> A[provider-a<br/>capacity 50]
  O --> B[provider-b<br/>capacity 30]
  O --> D[provider-c<br/>capacity 20]
  A --> SA[Attributed simulator]
  B --> SB[Attributed simulator]
  D --> SC[Attributed simulator]
  P[InferenceProvider capacityWeight] --> CRDT[Local and remote CRDT state]
  CRDT --> O
```

AGN applies capability, authorization, trust, health, freshness, and admission
checks before it forms selection groups. With `geographyFirst`, the closest
viable locality tier is active and remote tiers remain fallback; with
`scoreFirst`, eligible providers from different sites can share the active
group. Praxis reuses a permitted affinity binding first and performs the
weighted draw only for new, unbound requests inside the first viable group.
Relative weights are not percentages or request guarantees. This qualification
uses sessionless samples so affinity does not turn a random distribution into a
sticky sequence; affinity behavior itself remains unchanged.

```mermaid
flowchart LR
  E[Eligible providers] --> O[geographyFirst or scoreFirst]
  O --> G[First viable selection group]
  G --> A{Existing affinity?}
  A -->|Yes| R[Reuse provider]
  A -->|No| W[Apply static weights]
  W --> P[Select provider]
```

The policy is represented by:

```yaml
selectionPolicy:
  mode: weightedRandom
placementPolicy:
  strategy: static
```

Each `InferenceProvider.spec.capacityWeight` is a positive relative capacity;
`1` is the backward-compatible default. The three phases are:

1. Baseline `50/30/20`, rendered directly as `50/30/20`.
2. Changed `20/30/50`, rendered directly as `20/30/50` and requiring a new semantic
   revision without a gateway restart.
3. Equal `1/1/1`, rendered as equal weights and requiring another revision
   without a restart.

Before every sample, the runner records configured capacity, local and remote
CRDT state, rendered weights, and Grid/Praxis revisions. Traffic starts only
after two stable observations satisfy:

```text
Grid semantic revision == Praxis accepted revision == Praxis serving revision
```

Every request uses one persistent restricted client pod and records its ordinal,
status, selected provider, latency, attempts, and every transport-only retry.
Received HTTP errors and attribution mismatches are never retried. Statistical
acceptance uses a Pearson chi-square goodness-of-fit test with two degrees of
freedom and critical value `5.991` at `alpha = 0.05`. Random sampling can vary;
the test evaluates the complete distribution rather than requiring exact
percentages.

## Run the qualification

The qualification requires Docker, Kind, `kubectl`, Helm, OpenSSL, Cargo, and
a compatible Praxis AI gateway image. Run it in a dedicated checkout and do
not run concurrent Forge qualifications that share the same state or resource
names.

For source validation, build the Grid operator from this checkout and build a
compatible Praxis AI gateway image from its repository:

```console
# From the Grid repository root.
cargo build -p forge
IMAGE_TAG="grid-static-weighted-$(date -u +%Y%m%d%H%M%S)"
docker build -f deploy/operator/Containerfile -t "grid-operator:${IMAGE_TAG}" .

# From the compatible Praxis AI repository root.
docker build -f Containerfile -t "praxis-ai:${IMAGE_TAG}" .
```

Materialize the simulator and select the images explicitly. With
`GRID_XTASK_IMAGE_PULL_POLICY=Never`, every selected image must be present in
the local container store so the runner can load it into the run-owned Kind
clusters:

```console
docker pull ghcr.io/llm-d/llm-d-inference-sim:v0.10.2

export GRID_XTASK_OPERATOR_IMAGE="grid-operator:${IMAGE_TAG}"
export GRID_XTASK_GATEWAY_IMAGE="praxis-ai:${IMAGE_TAG}"
export GRID_XTASK_SIM_IMAGE=ghcr.io/llm-d/llm-d-inference-sim:v0.10.2
export GRID_XTASK_IMAGE_PULL_POLICY=Never

EVIDENCE_DIR="$(mktemp -d)"
cargo xtask env run-grid-static-weighted-qualification \
  --forge-config tests/e2e/topologies/grid-static-weighted/forge.yaml \
  --quick --teardown \
  --evidence-dir "$EVIDENCE_DIR"
```

Registry-hosted images may be used with immutable references and
`GRID_XTASK_IMAGE_PULL_POLICY=IfNotPresent`; set all image overrides to the
registry references in that case. Do not mix a local source image with an
unrelated registry image when validating one source revision.

The qualification records structured evidence for the selected source
revision. Each execution owns only the resources it creates, and teardown
removes those run-owned resources. For failures, compare configured, local,
remote, and rendered weights, then the three revision fields and gateway
identity before changing timeouts. The ordinary round-robin qualification
remains the compatibility check for non-weighted routing.
