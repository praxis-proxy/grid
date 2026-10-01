#!/usr/bin/env bash
# Checks one stage of the standalone praxis-gateway topology from inside the
# cluster. Forge runs it after each helm step; README.md lists the stages.
#
# Usage: verify.sh <default|inline-v1|inline-v2|existing|core-image|release>
#
# Env: KUBE_CONTEXT (required), GATEWAY_NAMESPACE (default praxis),
#      RELEASE (default praxis-gateway), CORE_IMAGE (required by core-image),
#      PRAXIS_GATEWAY_E2E_LOG (optional file collecting every check, since
#      Forge only shows a step's output when it fails).
set -euo pipefail

STAGE=${1:?usage: verify.sh <stage>}
CTX=${KUBE_CONTEXT:?KUBE_CONTEXT is required}
NS=${GATEWAY_NAMESPACE:-praxis}
RELEASE=${RELEASE:-praxis-gateway}
LOG=${PRAXIS_GATEWAY_E2E_LOG:-/dev/null}
ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)
CLIENT=praxis-client
PORT=8080

k() { kubectl --context "$CTX" -n "$NS" "$@"; }
h() { helm --kube-context "$CTX" -n "$NS" "$@"; }
log() { printf '%s\n' "$*" | tee -a "$LOG" >&2; }
pass() { log "  PASS [$STAGE] $1"; }
fail() { log "  FAIL [$STAGE] $1"; exit 1; }

# fetch <path>: status line, headers, and body as the in-cluster client sees them.
fetch() {
  k exec "$CLIENT" -- wget -S -q -O - -T 5 "http://${RELEASE}:${PORT}$1" 2>&1 || true
}

# matches <text> <ERE>...: every pattern matches somewhere, ignoring case.
matches() {
  local text=$1 pattern
  shift
  for pattern in "$@"; do
    grep -qiE -- "$pattern" <<<"$text" || return 1
  done
}

# expect <description> <path> <ERE>...: wait for six matching responses in a
# row. A draining pod may still answer with the previous config for a moment
# after the rollout, which only resets the streak, but a request that gets no
# HTTP response at all was dropped and fails the stage.
expect() {
  local desc=$1 path=$2 out="" streak=0 i
  shift 2
  for i in $(seq 1 120); do
    out=$(fetch "$path")
    if ! grep -qE 'HTTP/1\.[01] [0-9]{3}' <<<"$out"; then
      fail "$desc: GET $path got no HTTP response, so the gateway dropped it. Output: $out"
    fi
    if matches "$out" "$@"; then
      streak=$((streak + 1))
      if [ "$streak" -ge 6 ]; then
        pass "$desc"
        return 0
      fi
    else
      streak=0
    fi
    sleep 1
  done
  fail "$desc: GET $path never matched [$*] six times in a row. Last response: $out"
}

rollout() {
  k rollout status "deployment/$RELEASE" --timeout=180s >/dev/null \
    || fail "deployment/$RELEASE did not finish rolling out"
  pass "deployment/$RELEASE rolled out"
}

# praxis_value <jsonpath under the praxis container>: one field of its spec.
praxis_value() {
  k get deployment "$RELEASE" -o jsonpath="{.spec.template.spec.containers[?(@.name==\"praxis\")]$1}"
}

stage_default() {
  rollout
  expect "the built-in config answers GET / with a JSON status" / 'HTTP/1\.1 200' '"server": "praxis"'
  expect "the built-in config answers other paths with 404" /missing '404 Not Found'
  local uid
  uid=$(k exec "deployment/$RELEASE" -c praxis -- id -u)
  [ "$uid" = 100 ] || fail "praxis runs as UID $uid, want the image user 100"
  pass "praxis runs as the image's numeric user 100 under runAsNonRoot"
}

stage_inline_v1() {
  rollout
  expect "config.inline v1 proxies / to backend-a" / 'HTTP/1\.1 200' 'backend-a' 'x-config-version: v1'
}

stage_inline_v2() {
  rollout
  expect "config.inline v2 proxies / to backend-b" / 'HTTP/1\.1 200' 'backend-b' 'x-config-version: v2'
  expect "config.inline v2 answers /static itself" /static 'HTTP/1\.1 200' 'static from praxis'
}

stage_existing() {
  rollout
  expect "the existing ConfigMap proxies / to backend-a" / 'HTTP/1\.1 200' 'backend-a' 'x-config-version: existing'
  if k get configmap "$RELEASE-config" >/dev/null 2>&1; then
    fail "configmap/$RELEASE-config should be gone once config.existingConfigMap is set"
  fi
  pass "the chart removed its own ConfigMap"
}

stage_core_image() {
  local want=${CORE_IMAGE:?CORE_IMAGE is required for core-image} image command
  rollout
  image=$(praxis_value .image)
  command=$(praxis_value .command)
  [ "$image" = "$want" ] || fail "praxis container image is $image, want $want"
  [ "$command" = '["praxis"]' ] || fail "praxis container command is $command, want [\"praxis\"]"
  pass "the core Praxis image runs with command [praxis]"
  expect "the core Praxis image proxies / to backend-b" / 'HTTP/1\.1 200' 'backend-b' 'x-config-version: core'
}

stage_release() {
  h test "$RELEASE" --timeout 3m >/dev/null || fail "helm test $RELEASE failed"
  pass "helm test passes under the restricted pod security standard"

  local kinds crds namespaces notes description
  kinds=$(h get manifest "$RELEASE" | awk '$1 == "kind:" {print $2}' | sort -u | paste -sd ' ')
  [ "$kinds" = "ConfigMap Deployment Service" ] || fail "release manifest kinds are [$kinds], want [ConfigMap Deployment Service]"
  pass "the release holds only a ConfigMap, Deployment, and Service"

  crds=$(kubectl --context "$CTX" get crd -o name | { grep -c 'praxis-proxy\.io' || true; })
  [ "$crds" = 0 ] || fail "found $crds praxis-proxy.io CRDs; the chart must not need them"
  pass "no Grid CRDs exist in the cluster"

  namespaces=$(kubectl --context "$CTX" get pods -A -o jsonpath='{range .items[*]}{.metadata.namespace}{"\n"}{end}' \
    | sort -u | grep -vxE 'kube-system|local-path-storage|praxis|praxis-backends' || true)
  [ -z "$namespaces" ] || fail "unexpected workloads in: $namespaces"
  pass "nothing but the gateway, its backends, and Kubernetes itself is running"

  notes=$(h get notes "$RELEASE")
  if grep -qiE 'grid|\bAGN\b|operator|temporary' <<<"$notes"; then
    fail "the install notes mention Grid or operators: $notes"
  fi
  description=$(awk '/^description:/{f=1; next} f && /^[^ ]/{exit} f' "$ROOT/charts/praxis-gateway/Chart.yaml")
  if grep -qiE 'grid|\bAGN\b|temporary' <<<"$description"; then
    fail "the chart description mentions Grid: $description"
  fi
  pass "the chart description and install notes do not assume Grid"
}

case "$STAGE" in
  default) stage_default ;;
  inline-v1) stage_inline_v1 ;;
  inline-v2) stage_inline_v2 ;;
  existing) stage_existing ;;
  core-image) stage_core_image ;;
  release) stage_release ;;
  *) fail "unknown stage" ;;
esac
