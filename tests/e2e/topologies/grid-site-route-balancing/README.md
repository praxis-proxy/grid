# `grid_site_route` balancing qualification

This qualification measures the embedded Grid Gateway's `grid_site_route`
filter when it chooses among ordinary OpenAI-compatible inference endpoints.
It does not install or use llm-d EPP, queue-depth, KV-cache, utilization, or
other pressure telemetry. It reuses the single-cluster operator-derived
Gateway and provider lifecycle in
[`grid-operator-routing`](../grid-operator-routing/README.md); it does not
qualify `intelligent_route` or routing among Pods behind one Kubernetes
Service.

## Behavior of the embedded `grid_site_route` path

`grid_site_route` first limits selection to inference candidates matching the
requested model that are admitted, unexpired, and routable through the active
Gateway. It does not implement round robin. With no fresh pressure readings,
it uses a weighted draw. A candidate's draw weight is
`effective capacity * candidate weight`; unknown capacity is replaced by the
mean of known capacities among the eligible candidates, or `1` when all are
unknown. The Grid operator carries `InferenceProvider.spec.capacityWeight`
into candidate `weight`, including when `intelligent_route`'s static weighted
mode is not selected. With no measured capacity, equal configured weights
produce equal *expected* shares, and unequal `capacityWeight` values produce
shares proportional to those values. Individual requests remain probabilistic.

The Gateway excludes ineligible candidates before drawing. It does not move an
existing Responses or Conversations owner to another backend when that owner
is unavailable. Prefix affinity may narrow eligible sites that hold a prompt;
it does not apply to requests that continue a tagged Responses conversation.
`grid_site_route` balances Grid candidates (provider routes), while Kubernetes
or the provider's own proxy independently balances Pods behind an endpoint.

Prefix affinity needs at least four complete 256-byte canonical prompt blocks
(about 1 KiB) before a candidate can become sticky. A successful first response
confirms the selected candidate's prefix; the live affinity case disables
exploration and repeats a prompt long enough to cross that threshold. This
candidate affinity is separate from the tagged owner used by Responses or
Conversations continuation, and the prefix index is local to each Gateway
process.

`GridNetwork.spec.selectionPolicy` and `placementPolicy` describe the separate
`intelligent_route` path. They do not switch `grid_site_route` into deterministic,
round-robin, or static-weighted mode. The Gateway's rendered filter chain must
contain `grid_site_route`; this suite inspects that configuration and exercises
its request path.

## Supported-scenarios matrix

| Scenario | Current behavior and source | Qualification coverage | Scope and limits |
|---|---|---|---|
| One eligible endpoint | The sole admitted match receives every request. `route.rs`: `matching`, `select` | Live, after draining the other candidates; exact single-provider attribution and backend receipts | One Grid candidate; does not measure Service Pod balancing |
| Two equal candidates, no pressure | Weighted draw over candidate weights. `route.rs`: `select`, `weighted` | Live bounded sample; tolerance is statistical, not an exact split | No request or session affinity is reused in the sample |
| Three equal candidates, no pressure | Same draw; equal weights have equal expected shares | Live bounded sample; source unit regression also exercises three candidates | One site and one backend per provider |
| Unequal configured capacity | `capacityWeight` reaches `RouteCandidate.weight`; with unknown capacity its relative weight is preserved | Live bounded sample for unequal weights; unit test protects the distribution assertion | This is static relative capacity, not runtime pressure or a percentage guarantee |
| Absent pressure telemetry | No `metricsConfig` is installed; the candidate snapshot contains no queue, rho, or measured-capacity input | Live evidence records provider specs and the mounted snapshot before and after each sample | A missing metric is not injected as a zero-valued metric |
| Model isolation | Exact `kind` and model name are filtered before selection. `route.rs`: `matching` | Live: a second model is served only by its one configured provider; backend receipts check no other provider receives it | Controlled one-site provider set |
| Unknown model | No matching model returns the existing OpenAI-style 404 `model_not_found` | Live request and unit contract test | No status-code patch |
| Known model with no new-request candidate | Drained/Excluded candidates are not admitted for new traffic; the existing contract returns a retryable 503 for a known model with no admitted route | Live: drain all providers, observe 503 and no receipts, then restore; controlled tests distinguish unroutable candidates | Candidate remains in the serving snapshot while drained |
| Withdrawal and restoration | Administrative `trafficPolicy.drain` moves a provider to ExistingOnly; clearing it returns the provider to new traffic | Live: serving revision gate, no receipt while drained, successful attributed request after restore | This is an admission withdrawal, not a deleted InferenceProvider |
| Excluded, expired, unavailable, and unroutable candidates | `matching` rejects Excluded and expired inventory; route allowlist and health admission can make a candidate unroutable or unavailable | Controlled `RouteSnapshot` and route-response tests | Not presented as an operator live lifecycle |
| ExistingOnly vs NewAndExisting | Existing owner-tagged state may continue on ExistingOnly; new state may not | Unit regression plus live Responses continuation while owner drains | Live claim is limited to the controlled mock's stored-state contract |
| Stored Responses state | `previous_response_id` is parsed, owner-tagged response IDs are pinned, tags are stripped before forwarding | Live: create response, continue it, assert same provider receipt and upstream ID | Requires a taggable `resp_` fixture ID; arbitrary session headers are not used |
| Stored Conversations state | `conv_` IDs and `conversation` request fields have explicit parser/routing support | Live: create through `/v1/conversations`, read the returned ID, drain the owner, verify an ExistingOnly read, and verify restoration; the run records `ownerIdTagged: true` and backend receipts | The controlled fixture covers only this create/read path, not every Conversations API feature |
| Prefix affinity | A confirmed candidate prefix can narrow eligible candidates unless exploration or the queue gate overrides the match. At least four complete 256-byte canonical blocks are required. | Live: a successful warm-up with a prompt above the minimum is followed by the same prompt with `exploration: 0` and a one-count increase in the Gateway sticky decision metric. Focused Gateway tests cover same-site provider clusters and load overriding a match. | The prefix index is per Gateway process; short prompts and cross-Gateway cache sharing are outside this claim. Unique prompts are used for independent distribution samples. |
| Local and remote candidates | Local/remote telemetry and silence rules are applied before the selection tier; an unmeasured remote candidate is not equivalent to a measured zero | Controlled snapshot tests against local-site and remote-site candidates | The small live topology has one GridSite and does not qualify cross-site transport or locality policy |
| Normal JSON and SSE | Both are ordinary requests to the selected provider; Responses IDs are tagged only on successful, identity-encoded response bodies | Live valid Chat Completions JSON and streaming SSE with provider receipts | No throughput or token-rate claim |
| Bounded concurrent requests | Each independent request gets one receipt and valid attribution while the candidate revision stays fixed | The live distribution windows use eight bounded workers; revision and receipt snapshots bracket each window and every request is checked | This exercises bounded concurrency for routing attribution; it is not a standalone stress, throughput, or soak test |
| Passive endpoint outage and recovery | The selector's demotion and fallback behavior is covered by controlled `RouteSnapshot` tests in `route.rs` | Gateway filter unit tests; the dedicated site-route xtask does not scale a backend down | This is distinct from administrative `trafficPolicy.drain`; the existing operator-routing live outage phase is a separate command and is not part of this suite's run |

## Sample and evidence rules

Each distribution window records the candidate set, serving ConfigMap resource
version/digest, configured and eligible weights, expected probabilities, and provider receipt counters before
and after sampling. If the mounted serving revision changes, the window is
discarded and reported as inconclusive instead of mixing two candidate sets.
Each receipt window requires a stable recorder instance. Each request has a unique qualification ID. The sum of backend receipts must
equal the number of sent requests, and each request ID must occur exactly once;
the provider response marker must agree with `x-grid-site` and
`x-grid-backend`.

The full suite uses bounded samples selected so each expected provider receives
enough requests to distinguish a major weighting error while allowing random
variation. The quick suite reduces those counts but retains the same gates.
Distribution assertions use an explicit binomial confidence bound and have
focused unit tests for expected draws and deliberately skewed counts. They do
not require an exact split. Sample counts, expected probabilities, accepted
tolerances, candidate revision, and raw observations are written to the run's
`evidence.json` and bounded sample files.

## Run and retain the environment

Build the operator, Grid Gateway, and mock-provider images from the same
worktree. The runner uses `imagePullPolicy: Never`, so pass the exact built tags
through the required environment variables. Unique tags prevent reuse, and the
runner records local image IDs and each ready Pod's runtime `imageID`.

```console
SOURCE_TAG="$(git rev-parse --short HEAD)-$(date -u +%Y%m%dT%H%M%SZ)"
GRID_GIT_COMMIT="$(git rev-parse HEAD)"
if test -z "$(git status --porcelain)"; then
  GRID_GIT_TREE_STATE=clean
else
  GRID_GIT_TREE_STATE=dirty
fi
GRID_BUILD_DATE="$(date -u +%Y%m%d)"
BUILD_ARGS=(
  --build-arg "GRID_GIT_COMMIT=$GRID_GIT_COMMIT"
  --build-arg "GRID_GIT_TREE_STATE=$GRID_GIT_TREE_STATE"
  --build-arg "GRID_BUILD_DATE=$GRID_BUILD_DATE"
  --build-arg "CARGO_PROFILE=dev"
)

docker build "${BUILD_ARGS[@]}" -f deploy/operator/Containerfile \
  -t "grid-operator:site-route-qualification-$SOURCE_TAG" .
docker build "${BUILD_ARGS[@]}" -f deploy/gateway/Containerfile \
  -t "grid-gateway:site-route-qualification-$SOURCE_TAG" .
docker build "${BUILD_ARGS[@]}" -f crates/mock-providers/Containerfile \
  -t "grid-mock-providers:site-route-qualification-$SOURCE_TAG" .

CLIENT_IMAGE_REF="curlimages/curl@sha256:fcff5cf7a4b895da7bd2933c914938db2b05d2113fa0d6c55b6d29930408f661"
docker pull "$CLIENT_IMAGE_REF"
docker tag "$CLIENT_IMAGE_REF" curlimages/curl:8.10.1

export GRID_XTASK_OPERATOR_IMAGE="grid-operator:site-route-qualification-$SOURCE_TAG"
export GRID_XTASK_GRID_GATEWAY_IMAGE="grid-gateway:site-route-qualification-$SOURCE_TAG"
export GRID_XTASK_MOCK_PROVIDER_IMAGE="grid-mock-providers:site-route-qualification-$SOURCE_TAG"
export GRID_XTASK_IMAGE_PULL_POLICY=Never

EVIDENCE_DIR="$PWD/evidence/grid-site-route-balancing/$SOURCE_TAG"
cargo xtask env run-grid-site-route-balancing-qualification \
  --forge-config tests/e2e/topologies/grid-site-route-balancing/forge.yaml \
  --quick \
  --evidence-dir "$EVIDENCE_DIR/live"
```

Omitting `--quick` runs the full sample sizes. By default the runner leaves its
uniquely named Kind cluster and Forge state available. Omit both `--teardown`
and `--keep-on-failure` when you need the environment for inspection or later
performance testing. If you want this invocation to clean up after it finishes,
add `--teardown` to the original command; with `--teardown --keep-on-failure`,
a passing run is removed while run-owned resources from a failed run are retained
for diagnosis. If setup fails before Kind creates the cluster, the run report
records that no cluster or network remains. Do not
rerun the xtask with an earlier evidence directory to clean up a retained run:
each invocation creates a new run ID.

The runner refuses a pre-existing run-owned cluster or Docker network and does
not remove other Kind clusters or the shared Docker network. To clean up a
retained run later, set `RUN_EVIDENCE_DIR` to the exact `live` evidence
directory from that run, then use its recorded Forge config and state:

```console
RUN_EVIDENCE_DIR=/absolute/path/to/the/retained/run/live
RUN_CONFIG="$(jq -r '.observations.forge_working_config' "$RUN_EVIDENCE_DIR/evidence.json")"
RUN_STATE="$(jq -r '.observations.forge_state_dir' "$RUN_EVIDENCE_DIR/evidence.json")"
./scripts/forge.sh --config "$RUN_CONFIG" --state-dir "$RUN_STATE" --non-interactive down
```

This command targets the recorded run only. Verify its recorded `cluster_name`
and `forge_network_name` are absent before removing the exact `forge_state_dir`
and `forge_working_config` paths recorded in that evidence.

## What this suite does not establish

This suite does not qualify llm-d EPP pressure signals, queue-depth or KV-cache
transitions, multi-site remote transport, external providers, GPU capacity,
throughput, or latency. It distinguishes candidate selection from balancing
Pods inside a Kubernetes Service. It also does not establish `intelligent_route`
static/dynamic weighted selection, AIGC/MaaS routing, or behavior on a released
image with source different from the task build.
