#!/usr/bin/env bash
# praxis-gateway standalone e2e. Forge creates a Kind cluster with no Grid
# components, installs charts/praxis-gateway with no values, reconfigures it
# through config.inline, an existing ConfigMap, and the core Praxis image, and
# checks real requests after every change. The topology and its stages live in
# tests/e2e/topologies/praxis-gateway-standalone.
#
# Env: FORGE_BIN           explicit executable override (default: pinned upstream Forge)
#      STATE_DIR           Forge state directory
#                          (default target/forge/praxis-gateway-standalone)
#      KEEP=1              leave the cluster running afterwards
#      KIND_CREATE_PREFIX  command prefix for cluster creation, e.g. the rootless
#                          podman wrapper: systemd-run --scope --user -p Delegate=yes
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
cd "$ROOT"
CONFIG=tests/e2e/topologies/praxis-gateway-standalone/forge.yaml
STATE_DIR=${STATE_DIR:-$ROOT/target/forge/praxis-gateway-standalone}
CONTEXT=kind-praxis-standalone-gateway

FORGE_BIN=$("$ROOT/scripts/forge.sh" path)

forge() {
  "$FORGE_BIN" --config "$CONFIG" --state-dir "$STATE_DIR" --non-interactive "$@"
}

diagnostics() {
  local out=$STATE_DIR/diagnostics ns pod
  mkdir -p "$out"
  kubectl --context "$CONTEXT" get all,configmaps -A -o wide > "$out/resources.txt" 2>&1 || true
  kubectl --context "$CONTEXT" get events -A --sort-by=.lastTimestamp > "$out/events.txt" 2>&1 || true
  for ns in praxis praxis-backends; do
    for pod in $(kubectl --context "$CONTEXT" -n "$ns" get pods -o name 2>/dev/null); do
      kubectl --context "$CONTEXT" -n "$ns" describe "$pod" > "$out/$ns-${pod#pod/}.describe.txt" 2>&1 || true
      kubectl --context "$CONTEXT" -n "$ns" logs "$pod" --all-containers --tail=500 > "$out/$ns-${pod#pod/}.log" 2>&1 || true
    done
  done
  echo "Diagnostics written to $out"
}

teardown() {
  if [ "${KEEP:-0}" = 1 ]; then
    echo "KEEP=1: the cluster stays up as context $CONTEXT. Remove it with:"
    echo "  $FORGE_BIN --config $CONFIG --state-dir $STATE_DIR down --force"
  else
    forge down --force >/dev/null 2>&1 || true
  fi
}

mkdir -p "$STATE_DIR"
export PRAXIS_GATEWAY_E2E_LOG=$STATE_DIR/verify.log
: > "$PRAXIS_GATEWAY_E2E_LOG"
trap teardown EXIT

# up only creates the cluster; apply runs the cluster's stacks in order.
status=0
# shellcheck disable=SC2086 # KIND_CREATE_PREFIX is a command prefix and must word-split
${KIND_CREATE_PREFIX:-} "$FORGE_BIN" --config "$CONFIG" --state-dir "$STATE_DIR" --non-interactive up || status=$?
if [ "$status" = 0 ]; then
  forge apply gateway || status=$?
fi
cat "$PRAXIS_GATEWAY_E2E_LOG"
# A stack that never ran leaves no checks behind, so require the last stage's,
# and Forge keeps applying later stacks after one fails, so look for failures too.
if [ "$status" = 0 ] && { grep -q 'FAIL \[' "$PRAXIS_GATEWAY_E2E_LOG" \
  || ! grep -q 'PASS \[release\]' "$PRAXIS_GATEWAY_E2E_LOG"; }; then
  echo "a stage failed or never ran" >&2
  status=1
fi
if [ "$status" != 0 ]; then
  diagnostics
  echo "FAIL: praxis-gateway standalone e2e" >&2
  exit "$status"
fi
echo "PASS: praxis-gateway standalone e2e"
