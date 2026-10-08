#!/usr/bin/env bash
# Disposable CI proof of state handoff, relative assets, reuse, and failed up.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
FORGE_BIN=$("$ROOT/scripts/forge.sh" path)
: "${FORGE_BASELINE_BIN:?Set FORGE_BASELINE_BIN to the retired Grid CLI}"
[[ $FORGE_BASELINE_BIN == /* && -x $FORGE_BASELINE_BIN ]]
WORK=$(mktemp -d)
EVIDENCE=$ROOT/target/forge-compatibility/lifecycle
STATE=$WORK/state
CONFIG=$WORK/config/forge.yaml
NETWORK=grid-forge-migration-net
CLUSTER=grid-forge-migration-probe
OWNED=0
mkdir -p "$WORK/config" "$WORK/unrelated" "$EVIDENCE"
cp "$ROOT"/tests/e2e/forge-migration/* "$WORK/config/"
require_network_absent() {
  local networks
  networks=$(docker network ls --format '{{.Name}}')
  if grep -Fx "$NETWORK" <<< "$networks"; then
    printf 'Network must be absent: %s\n' "$NETWORK" >&2
    exit 1
  fi
}
require_cluster_absent() {
  local clusters
  clusters=$(kind get clusters)
  if grep -Fx "$CLUSTER" <<< "$clusters"; then
    printf 'Cluster must be absent: %s\n' "$CLUSTER" >&2
    exit 1
  fi
}
# This script is reserved for a disposable CI runner; never adopt live resources.
require_network_absent
require_cluster_absent
OWNED=1
cleanup() {
  if ((OWNED)); then
    "$FORGE_BIN" --config "$CONFIG" --state-dir "$STATE" --non-interactive down --force > "$EVIDENCE/cleanup.log" 2>&1 || true
    kind delete cluster --name "$CLUSTER" >> "$EVIDENCE/cleanup.log" 2>&1 || true
    docker network rm "$NETWORK" >> "$EVIDENCE/cleanup.log" 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT
forge() {
  "$FORGE_BIN" --config "$CONFIG" --state-dir "$STATE" --non-interactive "$@"
}
cd "$WORK/unrelated"
"$FORGE_BASELINE_BIN" --config "$CONFIG" --state-dir "$STATE" --non-interactive up
"$FORGE_BASELINE_BIN" --config "$CONFIG" --state-dir "$STATE" --non-interactive apply probe
[[ $(cat "$WORK/config/execution-cwd.txt") == "$WORK/unrelated" ]]
[[ $(cat "$WORK/config/execution-root.txt") == "$WORK/config" ]]
cp "$WORK/config/execution-cwd.txt" "$EVIDENCE/grid-execution-cwd.txt"
cp "$STATE/state.json" "$EVIDENCE/grid-state.json"
jq --exit-status '.networkCreatedByForge == true' "$STATE/state.json"
forge up
forge up
forge apply probe
[[ $(cat "$WORK/config/execution-cwd.txt") == "$WORK/unrelated" ]]
[[ $(cat "$WORK/config/execution-root.txt") == "$WORK/config" ]]
cmp "$WORK/config/execution-cwd.txt" "$EVIDENCE/grid-execution-cwd.txt"
[[ $(kubectl --context "kind-$CLUSTER" get configmap forge-migration-proof -o jsonpath='{.data.proof}') == upstream-compatible ]]
cp "$STATE/state.json" "$EVIDENCE/upstream-state.json"
jq --exit-status '.networkCreatedByForge == false and .networkId == null' "$STATE/state.json"
forge down --force
forge down --force
docker network inspect "$NETWORK" > "$EVIDENCE/preserved-legacy-network.json"
docker network rm "$NETWORK"

# Fresh upstream creation records the instance and survives repeated bring-up.
forge up
forge up
jq --exit-status '.networkCreatedByForge == true and (.networkId | length) == 64' "$STATE/state.json"
forge down --force
forge down --force
require_network_absent

# A matching label permits reuse, but must never confer creation ownership.
docker network create --label forge.managed=true --label forge.environment=grid-forge-migration "$NETWORK"
forge up
jq --exit-status '.networkCreatedByForge == false' "$STATE/state.json"
forge down --force
docker network inspect "$NETWORK" > "$EVIDENCE/preserved-network.json"
docker network rm "$NETWORK"

# Fail cluster creation after network allocation and prove tracked cleanup.
mkdir "$WORK/failing-tools"
REAL_KIND=$(command -v kind)
export REAL_KIND
KIND_CREATE_MARKER=$WORK/kind-create-called
export KIND_CREATE_MARKER
cat > "$WORK/failing-tools/kind" <<'SH'
#!/usr/bin/env bash
if [[ $1 == create ]]; then
  : > "$KIND_CREATE_MARKER"
  exit 77
fi
exec "$REAL_KIND" "$@"
SH
chmod +x "$WORK/failing-tools/kind"
if PATH="$WORK/failing-tools:$PATH" forge up > "$EVIDENCE/expected-up-failure.log" 2>&1; then
  printf 'Expected injected cluster creation failure\n' >&2
  exit 1
fi
[[ -f $KIND_CREATE_MARKER ]]
jq --exit-status \
  '.networkCreatedByForge == true and any(.clusters[]?; .name == "probe" and .phase == "creating")' \
  "$STATE/state.json"
cp "$STATE/state.json" "$EVIDENCE/failed-up-state.json"
forge down --force
require_network_absent
require_cluster_absent
printf 'State handoff, repeated up/down, relocation, reuse and failure cleanup passed\n'
