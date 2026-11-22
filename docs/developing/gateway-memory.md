# Gateway stream-memory qualification

This opt-in qualification measures the resident set size (RSS) of the Grid-owned
`grid-gateway` process while it proxies controlled OpenAI-compatible SSE streams.
It compares the Linux GNU/jemalloc build with the production Linux musl/system
allocator build. It does not measure cluster memory, inference throughput, or
llm-d/EPP signal routing.

The GNU/jemalloc and musl/system-allocator images were built from the same
dirty-source fingerprint,
`b26f96ae7c567722786bb8f168b39fdd40589439e1b70efa143a37ed352445dd`, at base
`fce66e6e083dbf8aed0142fb629c51aeb57f0565`. Three full trials per variant
passed the preselected 1.5 final-idle-to-warm-baseline ratio. All six runs
confirmed 1,024 simultaneous streams in each of eight bursts, used
`smaps_rollup:Rss` for every sample, drained clients and backend receipts, and
removed their Gateway containers successfully. Trial results appear below;
they qualify only this bounded local HTTP/1.1 profile.

Quick, cancellation, and slow-reader Docker smokes remain separate harness
checks. Earlier smokes confirmed two bursts of eight streams, 256 cancellation
requests with 64 cancellations, and four paused slow-reader streams. Those
smokes predate the full-profile fingerprint and are not part of the six-trial
memory result.

The runner also has an `observe` mode for the complete eight-burst,
1,024-stream workload. It performs the same resource and evidence checks as
`full`, records the final-to-baseline ratio, and omits the ratio gate. A
complete run is reported as `OBSERVATION_COMPLETE_NOT_QUALIFICATION`; both
`qualification_pass` and `qualification_claim` remain false. The mode does not
accept `--max-settle-ratio`. It is intended to collect complete measurements
before an accepted threshold is chosen, not to establish that a threshold
passes.

SIGTERM cancels the runner task once so its normal request, backend, sampler,
and Gateway cleanup can finish. The summary records the interruption as a
failure, so an interrupted observation is not reported as complete.

The runner's observation, SIGTERM, and exact-evidence checks are covered by
focused Python tests. The current full-profile runs used immutable images built
from the fingerprint above. This evidence does not claim inference throughput,
cluster RSS, or behavior under HTTP/2, TLS, or other request profiles.

## What the runner does

The runner starts one immutable Gateway image and a separate, barrier-controlled
SSE backend. The Gateway uses `grid_site_route` with one local candidate and no
peer signals, TLS, or EPP. Each request carries a unique run and burst ID. The
backend and clients separately confirm every live stream before the runner
records a plateau sample. Responses are parsed incrementally; prior SSE events
are not retained. The backend binds only to the Docker bridge gateway address;
it is not published to public host interfaces.

The full profile is fixed at eight bursts of 1,024 simultaneous streams. Each
stream remains open for a five-second plateau, then drains. The runner waits for
zero client and backend streams before measuring each ten-second burst settle
interval. It adds a ten-second idle gap between bursts and measures a 120-second
final-idle window after the eighth burst. It uses a four-CPU container cpuset, a
4 GiB memory limit, a 4 GiB memory-plus-swap limit, and a 4,096 process limit.
The exact observed CPU set, Gateway thread count, host memory, free disk, image
IDs, binary SHA-256, runtime libc, process identity, and measurement UID/GID are
recorded in evidence. The runner starts the Gateway as the invoking host user so
it can read that process's host `/proc` files without extra capabilities. The
UID must be nonzero; the measured Gateway remains nonroot.

The cold-ready value is retained separately. The warm idle baseline follows 16
concurrent warm-up requests, a fixed 20-second settle interval, and a ten-sample
one-second RSS window. The final RSS is the median of the last ten one-second
samples after two idle minutes. Every sample is written to `samples.jsonl`.
`smaps_rollup:Rss` is preferred; `status:VmRSS` is an explicitly recorded
fallback. The process start time from `/proc/<pid>/stat` is checked for every
sample to reject PID reuse.

The observed memory increment is:

```text
(median live-plateau RSS - warm idle baseline RSS) / confirmed simultaneous streams
```

It includes the Gateway runtime, proxy, and connections under this exact local
HTTP/1.1 workload. It is not a per-request allocation guarantee. GNU and musl
results stand on their own; a lower musl number is not a GNU failure.

## Build and qualify both variants

Run from the repository root on Linux x86_64 with Docker, `jq`, and at least
four available CPUs, 4,096 available file descriptors, 8 GiB host memory
available, and 10 GiB free in Docker storage for full mode. The build context
must remain unchanged from source fingerprint capture through both builds and
all trials. Recompute the source identity after any edit and rebuild both
images.

The fast runner tests do not require Docker:

```bash
PYTHONDONTWRITEBYTECODE=1 python3 -m unittest -v scripts.test_gateway_stream_memory
```

After building images for the current source fingerprint, an observation-only
run uses the full resource envelope and evidence contract without choosing an
RSS ratio limit:

```bash
python3 scripts/gateway_stream_memory.py \
  --mode observe --variant gnu-jemalloc --image "$GNU_IMAGE" \
  --source-revision "$SOURCE_REVISION" --run-id observe-gnu-01 \
  --evidence-dir "$EVIDENCE_ROOT/observe-gnu-01"
```

Observation mode requires exactly eight burst records and exactly 1,024
requested streams in each record. It also requires the full profile's live
concurrency, receipts, RSS sample windows, drain evidence, and verified
container cleanup. It exits successfully only when that evidence is complete.
The result remains observation-only even if its recorded ratio looks favorable.

```bash
DOCKER_ROOT="$(docker info --format '{{.DockerRootDir}}')"
free -h
df -h . "$DOCKER_ROOT"

SOURCE_JSON="$(python3 scripts/gateway_stream_memory.py --print-source-identity)"
SOURCE_REVISION="$(printf '%s' "$SOURCE_JSON" | jq -r .commit)"
SOURCE_FINGERPRINT="$(printf '%s' "$SOURCE_JSON" | jq -r .fingerprint_sha256)"
TREE_STATE="$(printf '%s' "$SOURCE_JSON" | jq -r 'if .dirty then "dirty" else "clean" end')"
GRID_VERSION="$(git describe --always --dirty)"
BUILD_DATE="$(date -u +%Y-%m-%dT%H:%M:%SZ)"
EVIDENCE_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/grid-memory-XXXXXX")"
GNU_TAG="grid-gateway-memory:gnu-${SOURCE_FINGERPRINT:0:12}"
MUSL_BASE_TAG="grid-gateway-memory:musl-base-${SOURCE_FINGERPRINT:0:12}"
MUSL_TAG="grid-gateway-memory:musl-${SOURCE_FINGERPRINT:0:12}"
set -euo pipefail

docker build \
  --progress=plain \
  --file crates/gateway/tests/memory/Containerfile.gnu \
  --build-arg "GRID_GIT_COMMIT=$SOURCE_REVISION" \
  --build-arg "GRID_GIT_VERSION=$GRID_VERSION" \
  --build-arg "GRID_GIT_TREE_STATE=$TREE_STATE" \
  --build-arg "GRID_BUILD_DATE=$BUILD_DATE" \
  --build-arg "GRID_QUALIFICATION_SOURCE_SHA256=$SOURCE_FINGERPRINT" \
  --tag "$GNU_TAG" . 2>&1 | tee "$EVIDENCE_ROOT/build-gnu.log"

docker build \
  --progress=plain \
  --file deploy/gateway/Containerfile \
  --build-arg CARGO_PROFILE=release \
  --build-arg "GRID_QUALIFICATION_SOURCE_SHA256=$SOURCE_FINGERPRINT" \
  --build-arg "GRID_GIT_COMMIT=$SOURCE_REVISION" \
  --build-arg "GRID_GIT_VERSION=$GRID_VERSION" \
  --build-arg "GRID_GIT_TREE_STATE=$TREE_STATE" \
  --build-arg "GRID_BUILD_DATE=$BUILD_DATE" \
  --tag "$MUSL_BASE_TAG" . 2>&1 | tee "$EVIDENCE_ROOT/build-musl.log"

MUSL_BASE_IMAGE_ID="$(docker image inspect --format '{{.Id}}' "$MUSL_BASE_TAG")"
printf '%s\n' "$MUSL_BASE_IMAGE_ID" > "$EVIDENCE_ROOT/musl-base-image-id-before-wrapper.txt"

docker build \
  --progress=plain \
  --file crates/gateway/tests/memory/Containerfile.musl \
  --build-arg "BASE_IMAGE=$MUSL_BASE_TAG" \
  --tag "$MUSL_TAG" . 2>&1 | tee "$EVIDENCE_ROOT/build-musl-lock.log"

GNU_IMAGE="$(docker image inspect --format '{{.Id}}' "$GNU_TAG")"
MUSL_IMAGE="$(docker image inspect --format '{{.Id}}' "$MUSL_TAG")"
MUSL_BASE_IMAGE_ID_AFTER="$(docker image inspect --format '{{.Id}}' "$MUSL_BASE_TAG")"
printf '%s\n' "$MUSL_BASE_IMAGE_ID_AFTER" > "$EVIDENCE_ROOT/musl-base-image-id-after-wrapper.txt"
test "$MUSL_BASE_IMAGE_ID" = "$MUSL_BASE_IMAGE_ID_AFTER" || exit 1
test "$(docker image inspect --format '{{json .RootFS.Layers}}' "$MUSL_BASE_TAG")" = \
  "$(docker image inspect --format '{{json .RootFS.Layers}}' "$MUSL_TAG")" || exit 1
docker image inspect "$MUSL_BASE_TAG" \
  | jq '.[0] | {id: .Id, repo_digests: .RepoDigests, repo_tags: .RepoTags}' \
  > "$EVIDENCE_ROOT/musl-build-image.json"

test "$(docker image inspect --format '{{index .Config.Labels "org.grid.qualification.source-sha256"}}' "$MUSL_IMAGE")" = \
  "$SOURCE_FINGERPRINT" || exit 1

for BASE_IMAGE in rust:1.96-bookworm debian:bookworm-slim rust:1.96-alpine alpine:3.23; do
  BASE_NAME="$(printf '%s' "$BASE_IMAGE" | tr ':/' '--')"
  docker buildx imagetools inspect "docker.io/library/$BASE_IMAGE" \
    > "$EVIDENCE_ROOT/base-${BASE_NAME}.txt"
done
```

The production musl image is built from its unchanged Containerfile. A test-only
metadata wrapper adds the source fingerprint and variant labels without changing
the binary or runtime layers. Pass its unique local base tag to `FROM`; a bare
`sha256:` image ID is interpreted as a registry reference by BuildKit. Record the
local base image ID immediately before and after wrapping and require it to stay
the same. Compare the base and wrapper `RootFS.Layers` to confirm that only
metadata changed. The runner rejects mutable image tags. Pass the
immutable local Docker image IDs captured above. Its preflight verifies the
image's commit, source fingerprint,
libc variant, allocator label, and linux/amd64 architecture. It verifies the
running container's image ID and Gateway PID, then records the binary digest and
startup identity. The GNU run also asks jemalloc to report its background-thread
configuration at process exit; missing that evidence fails the run.

First validate the runner against each image. Quick mode is a harness smoke and
cannot qualify a memory-retention threshold:

```bash
python3 scripts/gateway_stream_memory.py \
  --mode quick --variant gnu-jemalloc --image "$GNU_IMAGE" \
  --source-revision "$SOURCE_REVISION" --run-id quick-gnu \
  --evidence-dir "$EVIDENCE_ROOT/quick-gnu"
python3 scripts/gateway_stream_memory.py \
  --mode quick --variant musl-malloc --image "$MUSL_IMAGE" \
  --source-revision "$SOURCE_REVISION" --run-id quick-musl \
  --evidence-dir "$EVIDENCE_ROOT/quick-musl"
```

The cancellation and slow-reader modes validate separate streaming cleanup and
incremental-read paths. The latter confirms clients have seen a first SSE event
before the backend releases its tail, then keeps clients paused while a bounded
one-MiB-per-stream response is sent. Their RSS values are not included in the
primary comparison:

```bash
python3 scripts/gateway_stream_memory.py \
  --mode cancellation --variant gnu-jemalloc --image "$GNU_IMAGE" \
  --source-revision "$SOURCE_REVISION" --run-id cancel-gnu \
  --evidence-dir "$EVIDENCE_ROOT/cancel-gnu"
python3 scripts/gateway_stream_memory.py \
  --mode slow-reader --variant gnu-jemalloc --image "$GNU_IMAGE" \
  --source-revision "$SOURCE_REVISION" --run-id slow-reader-gnu \
  --evidence-dir "$EVIDENCE_ROOT/slow-reader-gnu"
```

After resource preflight, run at least three independent full trials for each
variant on the same host and profile. For the measured profile below, the 1.5
ratio was selected before these six trials and was not changed in response to
their outcomes. A different workload or threshold is a separate profile and
requires its own predeclared acceptance plan:

```bash
python3 scripts/gateway_stream_memory.py \
  --mode full --variant gnu-jemalloc --image "$GNU_IMAGE" \
  --source-revision "$SOURCE_REVISION" --max-settle-ratio 1.5 \
  --run-id full-gnu-01 --evidence-dir "$EVIDENCE_ROOT/full-gnu-01"
```

Repeat with unique run IDs and evidence directories, and run the same trials
with `--variant musl-malloc --image "$MUSL_IMAGE"`. Keep image builds and trials
serial so the comparison does not compete with itself for memory or CPU.

Each evidence directory is new and must not already exist. It contains the
source/host manifest, immutable image lock, generated Gateway configuration,
bounded Gateway log, one-second RSS samples, burst counters, cleanup status, and
summary. Quick, cancellation, and slow-reader outcomes are labeled as harness
passes; only a complete full trial can set `qualification_pass: true`.

## Interpret the result

The declared `--max-settle-ratio` compares final-idle RSS with the fixed warm
idle baseline. The runner fails on an exceeded ratio, incomplete stream counts,
missing or invalid RSS samples, a process identity change, active requests after
drain, an OOM kill, or cleanup errors. Review every trial, including failures;
do not infer a passing threshold from a mean or omit outliers.

Full mode applies the threshold to one profile and may mark that individual
trial as passed. The runner does not enforce three repeated trials or aggregate
them; the issue-level qualification requires reviewing every run in the
repeated set. For the six profiles below, every ratio passed the preselected
1.5 gate. It permits up to 50% residual RSS above the warm-idle baseline for
this exact workload, not a general leak-free guarantee. Observation mode can
collect a full profile without applying any threshold, but cannot mark it
qualified.

The completed profiles used base revision `fce66e6e083dbf8aed0142fb629c51aeb57f0565`
and source fingerprint
`b26f96ae7c567722786bb8f168b39fdd40589439e1b70efa143a37ed352445dd`. All six
profiles used the same immutable images, four-CPU cpuset, and 4 GiB memory and
memory-plus-swap limits. Every profile confirmed 1,024 client and backend
streams live in each of eight bursts, then drained all streams. Each used
`smaps_rollup:Rss` for every sample, including the ten samples used for its
final-idle median. The post-burst settle interval and inter-burst idle gap were
each ten seconds; the final-idle window was 120 seconds.

| Variant | Trial | Warm-idle median | Final-idle median | Final / warm | Result |
| --- | ---: | ---: | ---: | ---: | --- |
| GNU / jemalloc | 1 | 37.12 MiB | 43.71 MiB | 1.1774 | Pass |
| GNU / jemalloc | 2 | 37.44 MiB | 44.62 MiB | 1.1919 | Pass |
| GNU / jemalloc | 3 | 37.07 MiB | 44.11 MiB | 1.1900 | Pass |
| musl / system allocator | 1 | 30.59 MiB | 32.09 MiB | 1.0490 | Pass |
| musl / system allocator | 2 | 30.87 MiB | 32.44 MiB | 1.0509 | Pass |
| musl / system allocator | 3 | 30.77 MiB | 32.29 MiB | 1.0495 | Pass |

Final-idle RSS was 17.7–19.2% above warm baseline for GNU and 4.9–5.1% above
for musl. The live-plateau memory increment per confirmed stream ranged from
80,596–92,156 bytes for GNU and 94,252–94,832 bytes for musl. These values
cover only this bounded local HTTP/1.1 SSE profile. Raw samples, per-burst
receipts, image locks, and cleanup records are retained in an evidence directory
for each trial.

## Boundaries

- This workload uses local HTTP/1.1 without TLS. TLS and HTTP/2 are separate
  qualifications.
- The backend is synthetic and validates request identity and stream lifecycle;
  it does not perform inference.
- The test does not measure Kubernetes, Kind, cgroups as a substitute for
  process RSS, throughput, or production load.
- A signals-based Kind xtask may exercise Gateway behavior in a cluster, but its
  cluster RSS cannot replace or be combined with these process-level trials.
- The qualification is opt-in and is not invoked from GitHub Actions.
- Do not change allocator behavior or proxy buffer sizes to make the gate pass.

### Source provenance checks

The source-identity command rejects Git-ignored inputs that Docker might send,
including an ignored `.cargo/config`. Remove or track those files before building.
Known exclusions in the root `.dockerignore`, such as `target` and `evidence`,
are allowed. This is a conservative check: unsupported exclusion patterns do
not excuse ignored files, and negated rules disable the exclusion shortcut.
Dockerfile-specific ignore overrides are rejected because they would replace the
root build-context policy.

The musl binary-producing build records the source fingerprint; the wrapper
inherits it and cannot substitute a different fingerprint. Qualification rejects
an unknown or mismatched label. These labels and saved build commands document
build provenance; they are not cryptographic attestations of the source content.
