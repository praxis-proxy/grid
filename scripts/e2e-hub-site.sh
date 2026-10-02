#!/usr/bin/env bash
# Hub and site install e2e: forge creates a hub and a site kind cluster, then the script
# runs the examples/helm/hub-site README commands, with test-only overrides, and asserts
# enrollment, membership, serving, peer trust, and log health per trust mode.
#
# Usage: e2e-hub-site.sh [all|up|test|down]   (default all)
#   up     build and load the images, create the clusters, apply the topology stacks
#   test   per mode in MODES: reset the grid namespace, install, assert
#   down   delete the clusters
#
# Env:
#   MODES               peer trust modes to test (default "pin spiffe")
#   IMAGE_PREFIX        image repository prefix (default localhost/grid-)
#   IMAGE_TAG           image tag (default e2e)
#   SKIP_BUILD=1        use prebuilt <prefix>{operator,gateway,enrollment}:<tag> images
#   CONTAINER_TOOL      image build tool (default docker)
#   FORGE_BIN           praxis-forge binary (default: PATH, then target/debug, then cargo build)
#   KIND_CREATE_PREFIX  command prefix for cluster creation, e.g. the rootless podman
#                       wrapper: systemd-run --scope --user -p Delegate=yes
#   KEEP=1              keep clusters this run created
#   ARTIFACTS           log directory (default /tmp/grid-hub-site-e2e)
#   TIMEOUT             seconds to wait for each converging condition (default 180)
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
TOPO="$ROOT/tests/e2e/topologies/grid-hub-site"
MODES=${MODES:-pin spiffe}
IMAGE_PREFIX=${IMAGE_PREFIX:-localhost/grid-}
IMAGE_TAG=${IMAGE_TAG:-e2e}
CONTAINER_TOOL=${CONTAINER_TOOL:-docker}
ARTIFACTS=${ARTIFACTS:-/tmp/grid-hub-site-e2e}
TIMEOUT=${TIMEOUT:-180}
PREFIX=grid-hub-site
HUB_CTX=kind-$PREFIX-hub
SITE_CTX=kind-$PREFIX-site
NS=grid
ENS=grid-enrollment
SITE="site-a"
MODEL=Qwen/Qwen3-0.6B
ENROLL_HOST=enroll.grid.example.com
ENROLL_PORT=8443
FORGE_STATE="$ROOT/.forge/$PREFIX"
# Rootless podman's kind provider reads its own network variable.
export KIND_EXPERIMENTAL_PODMAN_NETWORK=${KIND_EXPERIMENTAL_PODMAN_NETWORK:-$PREFIX-net}

WORK=$(mktemp -d)
PF_PID=""
FAILED=0
# Start of the deliberate-refusal checks; assert_health allows gateway handshake rejections after it.
REFUSALS_FROM=""
CREATED=0
MODE=""

cleanup() {
  [[ -z $PF_PID ]] || kill "$PF_PID" 2>/dev/null || true
  if ((CREATED)) && [[ ${KEEP:-0} != 1 ]]; then
    log "deleting clusters this run created"
    forge down --force >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

log() { printf '==> %s\n' "$*" >&2; }
pass() { printf 'PASS [%s] %s\n' "$MODE" "$*"; }
fail() {
  printf 'FAIL [%s] %s\n' "$MODE" "$*"
  FAILED=1
}
die() {
  printf 'FAIL [%s] %s\n' "${MODE:-setup}" "$*"
  exit 1
}

k() {
  local ctx=$1
  shift
  kubectl --context "$ctx" -n "$NS" "$@"
}

forge() {
  "$FORGE_BIN" --config "$TOPO/forge.yaml" --state-dir "$FORGE_STATE" --non-interactive "$@"
}

resolve_forge() {
  if [[ -n ${FORGE_BIN:-} ]]; then
    return
  fi
  if command -v praxis-forge >/dev/null; then
    FORGE_BIN=praxis-forge
    return
  fi
  FORGE_BIN="$ROOT/target/debug/praxis-forge"
  [[ -x $FORGE_BIN ]] || cargo build --manifest-path "$ROOT/Cargo.toml" -p forge --bin praxis-forge
}

cluster_exists() { kind get clusters 2>/dev/null | grep -qx "$PREFIX-$1"; }

image() { printf '%s%s:%s' "$IMAGE_PREFIX" "$1" "$IMAGE_TAG"; }

cmd_up() {
  local c
  for c in hub site; do
    ! cluster_exists "$c" || die "kind cluster $PREFIX-$c exists; refusing to reuse it. Run: $0 down"
  done
  if [[ ${SKIP_BUILD:-0} != 1 ]]; then
    for c in operator gateway enrollment; do
      log "building $(image "$c")"
      "$CONTAINER_TOOL" build -f "$ROOT/deploy/$c/Containerfile" -t "$(image "$c")" "$ROOT"
    done
  fi
  resolve_forge
  CREATED=1
  # Word splitting is the point: the prefix is a command and its flags.
  # shellcheck disable=SC2086
  ${KIND_CREATE_PREFIX:-} "$FORGE_BIN" --config "$TOPO/forge.yaml" --state-dir "$FORGE_STATE" \
    --non-interactive up
  for c in hub site; do
    for img in operator gateway enrollment; do
      forge cluster load-image "$c" "$(image "$img")"
    done
    forge apply "$c"
  done
}

cmd_down() {
  resolve_forge
  forge down --force
}

# ip_add <ipv4> <n>
ip_add() {
  local a b c d n
  IFS=. read -r a b c d <<<"$1"
  n=$(((a << 24 | b << 16 | c << 8 | d) + $2))
  printf '%d.%d.%d.%d' $((n >> 24 & 255)) $((n >> 16 & 255)) $((n >> 8 & 255)) $((n & 255))
}

pool_start() {
  kubectl --context "$1" -n metallb-system get ipaddresspool forge-pool -o jsonpath='{.spec.addresses[0]}' | cut -d- -f1
}

# Fixed LoadBalancer addresses, so every URL and seed is known before install.
plan_addresses() {
  local hub site
  hub=$(pool_start "$HUB_CTX")
  site=$(pool_start "$SITE_CTX")
  [[ -n $hub && -n $site ]] || die "no MetalLB pool on the clusters; run: $0 up"
  ENROLL_IP=$(ip_add "$hub" 1)
  HUB_SWIM_IP=$(ip_add "$hub" 2)
  SITE_SWIM_IP=$(ip_add "$site" 1)
  SITE_GW_IP=$(ip_add "$site" 2)
  MODEL_IP=$(kubectl --context "$SITE_CTX" -n model get service vcr-inference-site -o jsonpath='{.spec.clusterIP}')
  [[ -n $MODEL_IP ]] || die "no vcr-inference-site Service on the site"
  log "enrollment $ENROLL_IP, hub SWIM $HUB_SWIM_IP, site SWIM $SITE_SWIM_IP, site gateway $SITE_GW_IP"
}

# helm_on <context> <namespace> <release> <chart> [--set flags...]: the README command
# plus the test-only overrides the caller appends.
helm_on() {
  local ctx=$1 ns=$2 release=$3 chart=$4
  shift 4
  log "[$MODE] helm upgrade --install $release ($ctx)"
  helm upgrade --install "$release" "$ROOT/charts/$chart" --kube-context "$ctx" -n "$ns" --create-namespace \
    --timeout 10m "$@" >/dev/null
}

# images <component>: this run's image for a chart, into IMG.
images() { IMG=(--set-string "image.repository=$IMAGE_PREFIX$1" --set-string "image.tag=$IMAGE_TAG"); }

# leaf_digest <context>: the README command.
leaf_digest() {
  kubectl --context "$1" -n "$NS" get secret grid-site-identity -o jsonpath='{.data.tls\.crt}' \
    | base64 -d | openssl x509 -outform DER | openssl dgst -sha256 -r | cut -d' ' -f1
}

enrolled() { k "$1" get secret grid-site-identity grid-ca; }

# snapshot_logs <context> <label>: every container, init and hook pods included.
# Returns nonzero when any listing or log fetch fails.
snapshot_logs() {
  local dir="$ARTIFACTS/$MODE/$1" pods pod ns rc=0
  mkdir -p "$dir"
  for ns in "$NS" "$ENS"; do
    pods=$(kubectl --context "$1" -n "$ns" get pods -o jsonpath='{.items[*].metadata.name}') || { rc=1; continue; }
    for pod in $pods; do
      kubectl --context "$1" -n "$ns" logs "$pod" --all-containers --prefix >"$dir/$2-$pod.log" 2>&1 || rc=1
    done
  done
  return "$rc"
}

# uninstall <context> <namespace> <release...>
uninstall() {
  local ctx=$1 ns=$2 rel
  shift 2
  for rel in "$@"; do
    if helm --kube-context "$ctx" -n "$ns" status "$rel" >/dev/null 2>&1; then
      helm --kube-context "$ctx" -n "$ns" uninstall "$rel" --wait --timeout 5m >/dev/null
    fi
  done
}

reset_grid() {
  local ctx
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    # Grid resources first, while the operator can still drop its finalizers.
    uninstall "$ctx" "$NS" grid-gateway grid-site
    if kubectl --context "$ctx" get crd gridsites.grid.praxis.fast >/dev/null 2>&1; then
      k "$ctx" delete gridsites,gridnetworks,inferenceproviders --all --wait --timeout 2m >/dev/null || true
    fi
    k "$ctx" delete gridoperators --all --ignore-not-found >/dev/null 2>&1 || true
    uninstall "$ctx" "$NS" grid-operator
    uninstall "$ctx" "$ENS" grid-enrollment
    kubectl --context "$ctx" delete namespace "$NS" "$ENS" --ignore-not-found --wait --timeout 5m >/dev/null \
      || fail "reset: namespaces $NS and $ENS on $ctx not deleted"
  done
}

# eventually <what> <command...>: retry until the command succeeds or TIMEOUT.
eventually() {
  local what=$1 deadline=$((SECONDS + TIMEOUT))
  shift
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      fail "$what (not within ${TIMEOUT}s)"
      return 1
    fi
    sleep 5
  done
  pass "$what"
}

strip_ansi() { sed 's/\x1b\[[0-9;]*m//g'; }

# copy_key <from context> <from namespace> <to context> <secret> <key> [site]: the README
# copy of one Secret key, with the invite's site label.
copy_key() {
  local from=$1 ns=$2 to=$3 name=$4 key=$5 site=${6:-}
  kubectl --context "$from" -n "$ns" get secret "$name" -o jsonpath="{.data.${key//./\\.}}" | base64 -d \
    | kubectl --context "$to" -n "$NS" create secret generic "$name" --from-file="$key=/dev/stdin" >/dev/null || return 1
  [[ -z $site ]] || kubectl --context "$to" -n "$NS" label secret "$name" "grid.praxis.fast/site=$site" >/dev/null
}

# Test-only overrides: this run's images, fixed LoadBalancer addresses, the mock model,
# the trust mode, a rogue invite, and the hub gateway ref the overlay assert reads.
install_hub() {
  images enrollment
  helm_on "$HUB_CTX" "$ENS" grid-enrollment grid-enrollment \
    --set host="$ENROLL_HOST" --set invites.hub.network=grid --set "invites.$SITE.network=grid" \
    "${IMG[@]}" --set enrollment.service.loadBalancerIP="$ENROLL_IP" --set invites.rogue.network=grid \
    || die "install grid-enrollment"
  images operator
  helm_on "$HUB_CTX" "$NS" grid-operator grid-operator \
    --set swim.siteName=hub --set enrollment.enabled=true \
    "${IMG[@]}" --set swim.service.loadBalancerIP="$HUB_SWIM_IP" || die "install hub grid-operator"
  copy_key "$HUB_CTX" "$ENS" "$HUB_CTX" grid-ca-bundle ca.crt || die "copy the hub CA bundle"
  copy_key "$HUB_CTX" "$ENS" "$HUB_CTX" grid-invite-hub token hub || die "copy the hub invite"
  head -c 32 /dev/urandom | k "$HUB_CTX" create secret generic grid-swim-key --from-file=key=/dev/stdin >/dev/null \
    || die "create grid-swim-key"
  HUB_SITE_ARGS=(--set gridNetwork.gridId="$PREFIX-e2e" --set gridSite.name=hub
    --set "peers.$SITE.address=$SITE_GW_IP:8080"
    --set gridNetwork.peerTrust.mode="$MODE" --set "gridNetwork.gatewayRefs[0].name=grid-gateway"
    --set "gridNetwork.gatewayRefs[0].namespace=$NS" --set "gridNetwork.gatewayRefs[0].localSiteName=hub")
  helm_on "$HUB_CTX" "$NS" grid-site grid-site "${HUB_SITE_ARGS[@]}" || die "install hub grid-site"
  images gateway
  helm_on "$HUB_CTX" "$NS" grid-gateway praxis-gateway \
    --set gatewayConfig.localSite=hub --set gatewayConfig.model="$MODEL" --set gatewayConfig.auth.mode=none \
    --set "gatewayConfig.backends.$SITE.endpoint=$SITE_GW_IP:8080" "${IMG[@]}" || die "install hub grid-gateway"
  eventually "hub operator enrolled from its own invite through the in-cluster Service" enrolled "$HUB_CTX" \
    || die "hub not enrolled"
  HUB_DIGEST=$(leaf_digest "$HUB_CTX")
  [[ $HUB_DIGEST =~ ^[0-9a-f]{64}$ ]] || die "no hub leaf digest"
  snapshot_logs "$HUB_CTX" hub || true
}

# Test fixture: the example's enrollment name resolves only off the test network, so
# the site reaches the hub LoadBalancer through a selectorless Service on the site.
enrollment_forward() {
  kubectl --context "$SITE_CTX" create namespace "$ENS" --dry-run=client -o yaml | kubectl --context "$SITE_CTX" apply -f - >/dev/null
  kubectl --context "$SITE_CTX" -n "$ENS" apply -f - >/dev/null <<EOF
apiVersion: v1
kind: Service
metadata:
  name: grid-enrollment
spec:
  ports:
    - name: https
      port: $ENROLL_PORT
---
apiVersion: discovery.k8s.io/v1
kind: EndpointSlice
metadata:
  name: grid-enrollment-hub
  labels:
    kubernetes.io/service-name: grid-enrollment
addressType: IPv4
ports:
  - name: https
    port: $ENROLL_PORT
endpoints:
  - addresses: ["$ENROLL_IP"]
EOF
}

install_site() {
  local trust
  images operator
  helm_on "$SITE_CTX" "$NS" grid-operator grid-operator \
    --set "swim.siteName=$SITE" --set "swim.seeds=$HUB_SWIM_IP:7946" \
    --set enrollment.enabled=true --set "enrollment.url=https://grid-enrollment.$ENS.svc:$ENROLL_PORT" \
    "${IMG[@]}" --set swim.service.loadBalancerIP="$SITE_SWIM_IP" || die "install site grid-operator"
  enrollment_forward || die "enrollment forward"
  copy_key "$HUB_CTX" "$ENS" "$SITE_CTX" grid-ca-bundle ca.crt || die "copy the site CA bundle"
  copy_key "$HUB_CTX" "$ENS" "$SITE_CTX" "grid-invite-$SITE" token "$SITE" || die "copy the site invite"
  copy_key "$HUB_CTX" "$NS" "$SITE_CTX" grid-swim-key key || die "copy the SWIM key"
  if [[ $(k "$SITE_CTX" get secret "grid-invite-$SITE" -o jsonpath='{.metadata.labels.grid\.praxis\.fast/site}') == "$SITE" ]]; then
    pass "copied grid-ca-bundle, grid-invite-$SITE (site label set), and grid-swim-key to the site"
  else
    fail "copied invite lacks its grid.praxis.fast/site label"
  fi
  helm_on "$SITE_CTX" "$NS" grid-site grid-site \
    --set gridNetwork.gridId="$PREFIX-e2e" --set "gridSite.name=$SITE" --set "peers.hub.digest=$HUB_DIGEST" \
    --set "inferenceProviders.vcr.endpoint=http://$MODEL_IP:8000" \
    --set "inferenceProviders.vcr.model=$MODEL" --set gridNetwork.peerTrust.mode="$MODE" || die "install site grid-site"
  trust=(--set "gatewayConfig.peerTrust.digest=$HUB_DIGEST")
  [[ $MODE == pin ]] || trust=(--set gatewayConfig.peerTrust.mode=spiffe
    --set gatewayConfig.peerTrust.spiffeId=spiffe://grid.internal/site/hub)
  images gateway
  helm_on "$SITE_CTX" "$NS" grid-gateway praxis-gateway \
    --set gatewayConfig.role=provider --set "gatewayConfig.localSite=$SITE" "${trust[@]}" \
    --set "gatewayConfig.backends.local.endpoint=$MODEL_IP:8000" \
    "${IMG[@]}" --set service.loadBalancerIP="$SITE_GW_IP" || die "install site grid-gateway"
  eventually "site operator enrolled from its copied invite through the hub enrollment URL" enrolled "$SITE_CTX" \
    || die "site not enrolled"
  helm_on "$HUB_CTX" "$NS" grid-site grid-site "${HUB_SITE_ARGS[@]}" \
    --set "peers.$SITE.digest=$(leaf_digest "$SITE_CTX")" || die "pin the site on the hub"
  snapshot_logs "$SITE_CTX" site || true
}

install_grid() {
  local ctx
  install_hub
  install_site
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    k "$ctx" rollout status deployment --timeout 5m >/dev/null || die "deployments on $ctx not ready"
  done
  pass "hub and site installed with helm, every deployment rolled out"
}

spiffe_id_of() { # <context>
  k "$1" get secret grid-site-identity -o jsonpath='{.data.tls\.crt}' | base64 -d \
    | openssl x509 -noout -ext subjectAltName | grep -o 'URI:spiffe://[^, ]*' | cut -d: -f2-
}

network_active() {
  k "$1" get gridnetwork grid -o json | jq -e '.status.phase == "Active" and (.status.connectedSites // 0) >= 1'
}

site_phase_in() { # <context> <site> <phase ERE>
  k "$1" get gridsite "$2" -o jsonpath='{.status.phase}' | grep -qxE "$3"
}

overlay_distributed() {
  k "$HUB_CTX" get gridnetwork grid -o json | jq -e '[.status.overlayStatus[]?
    | select(.gatewayName == "grid-gateway" and .phase == "Distributed" and .candidateCount >= 1)] | length == 1'
}

gridoperator_available() { # <context>
  k "$1" get gridoperator cluster -o json | jq -e '[.status.conditions[]
    | select((.type == "Available" and .status == "True") or (.type == "Degraded" and .status == "False"))] | length == 2'
}

assert_membership() {
  local ctx name
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    name=hub
    [[ $ctx == "$HUB_CTX" ]] || name=$SITE
    if [[ $(spiffe_id_of "$ctx") == "spiffe://grid.internal/site/$name" ]]; then
      pass "$name identity carries spiffe://grid.internal/site/$name"
    else
      fail "$name identity SPIFFE ID: $(spiffe_id_of "$ctx")"
    fi
  done
  eventually "hub GridSite grid-$SITE Active (identity-verified probe)" site_phase_in "$HUB_CTX" "grid-$SITE" Active || true
  # The hub gateway is ClusterIP, so the hub gossips no gateway address and stays Discovered on the site.
  eventually "site GridSite grid-hub Discovered over SWIM" site_phase_in "$SITE_CTX" grid-hub Discovered || true
  eventually "hub GridNetwork Active with >=1 connected site" network_active "$HUB_CTX" || true
  # Operator state only: the hub gateway routes on its static backends, not this overlay.
  eventually "operator renders the hub overlay with a remote candidate" overlay_distributed || true
  eventually "hub GridOperator cluster Available, not Degraded" gridoperator_available "$HUB_CTX" || true
  eventually "site GridOperator cluster Available, not Degraded" gridoperator_available "$SITE_CTX" || true
}

# site_call <out prefix> <path> [curl args...]: direct TLS call to the site gateway, the
# path sent as is. Prints the HTTP status and leaves curl's exit code in <out>.rc.
site_call() {
  local out=$1 path=$2 rc=0
  shift 2
  curl -sS --path-as-is -o "$out.body" -D "$out.hdr" -w '%{http_code}' --max-time 15 \
    --resolve "$SITE.grid.internal:8080:$SITE_GW_IP" --cacert "$WORK/id/ca.crt" \
    -H 'Content-Type: application/json' "$@" "https://$SITE.grid.internal:8080$path" 2>"$out.err" || rc=$?
  echo "$rc" >"$out.rc"
}

# tls_refused <out prefix>: the handshake failed with a TLS alert, curl exit 35 or 56.
tls_refused() {
  [[ $(cat "$1.rc") == 35 || $(cat "$1.rc") == 56 ]] && grep -qiE 'alert|certificate required' "$1.err"
}

# refused_path <label> <want code> <path> [curl args...]: hub identity, no provider header.
refused_path() {
  local label=$1 want=$2 path=$3 out="$WORK/path" code
  shift 3
  code=$(site_call "$out" "$path" --cert "$WORK/id/hub.crt" --key "$WORK/id/hub.key" "$@")
  if [[ $code == "$want" ]] && ! grep -qi '^x-grid-provider-site:' "$out.hdr"; then
    pass "$label: $code, no provider header"
  else
    fail "$label: HTTP $code, want $want with no x-grid-provider-site"
  fi
}

chat_body() { jq -cn --arg m "$MODEL" '{model: $m, messages: [{role: "user", content: "ping"}], max_tokens: 8}'; }

# served <out prefix>: the site's provider header and a mock model completion.
served() {
  local out=$1
  grep -qi "^x-grid-provider-site: $SITE" "$out.hdr" && jq -e '.choices | length > 0' "$out.body" >/dev/null
}

# enroll_rogue: redeem the rogue invite with a local key, a Grid-CA identity no peer trusts.
enroll_rogue() {
  local d="$WORK/id" token
  token=$(kubectl --context "$HUB_CTX" -n "$ENS" get secret grid-invite-rogue -o jsonpath='{.data.token}' | base64 -d)
  openssl ecparam -name prime256v1 -genkey -noout -out "$d/rogue.key" 2>/dev/null
  openssl req -new -key "$d/rogue.key" -subj "/CN=rogue" -out "$d/rogue.csr" 2>/dev/null
  jq -n --rawfile csr "$d/rogue.csr" '{csr: $csr}' \
    | curl -sS --fail-with-body --resolve "$ENROLL_HOST:$ENROLL_PORT:$ENROLL_IP" --cacert "$d/ca.crt" \
      -H @<(printf 'Authorization: Bearer %s\n' "$token") -H 'Content-Type: application/json' -d @- \
      "https://$ENROLL_HOST:$ENROLL_PORT/v1alpha1/enrollments" | jq -er .certificate >"$d/rogue.crt"
}

consumer_served() {
  local out="$WORK/consumer"
  [[ $(chat_body | curl -sS -o "$out.body" -D "$out.hdr" -w '%{http_code}' --max-time 15 \
    -H 'Content-Type: application/json' -d @- http://127.0.0.1:18080/v1/chat/completions) == 200 ]] \
    && served "$out"
}

assert_serving() {
  local d="$WORK/id" code
  rm -rf "$d"
  mkdir -p "$d"
  k "$HUB_CTX" get secret grid-ca -o jsonpath='{.data.ca\.crt}' | base64 -d >"$d/ca.crt"
  k "$HUB_CTX" get secret grid-site-identity -o jsonpath='{.data.tls\.crt}' | base64 -d >"$d/hub.crt"
  k "$HUB_CTX" get secret grid-site-identity -o jsonpath='{.data.tls\.key}' | base64 -d >"$d/hub.key"

  k "$HUB_CTX" port-forward service/grid-gateway 18080:8080 >"$WORK/pf.log" 2>&1 &
  PF_PID=$!
  eventually "hub consumer gateway serves a chat completion over its static site backend (x-grid-provider-site: $SITE)" \
    consumer_served || cat "$WORK/consumer.hdr" "$WORK/consumer.body" 2>/dev/null || true
  kill "$PF_PID" 2>/dev/null || true
  PF_PID=""

  code=$(chat_body | site_call "$WORK/direct" /v1/chat/completions --cert "$d/hub.crt" --key "$d/hub.key" -d @-)
  if [[ $code == 200 ]] && served "$WORK/direct"; then
    pass "hub identity over mTLS: 200, x-grid-provider-site: $SITE, mock completion"
  else
    fail "hub identity over mTLS: HTTP $code $(head -c 300 "$WORK/direct.body" 2>/dev/null)"
  fi

  REFUSALS_FROM=$(date -u +%Y-%m-%dT%H:%M:%S)
  code=$(chat_body | site_call "$WORK/nocert" /v1/chat/completions -d @-)
  if tls_refused "$WORK/nocert"; then
    pass "anonymous caller: TLS alert, curl exit $(cat "$WORK/nocert.rc") ($(tr '\n' ' ' <"$WORK/nocert.err" | cut -c1-100))"
  else
    fail "anonymous caller: HTTP $code, curl exit $(cat "$WORK/nocert.rc"), want a TLS alert"
  fi

  # The backend serves /health, so a 404 here is the gateway refusing it.
  refused_path "disallowed path /health" 404 /health
  refused_path "traversal /v1/models/../../health" 404 /v1/models/../../health
  refused_path "encoded traversal /v1/models/%2e%2e/%2e%2e/health" 404 /v1/models/%2e%2e/%2e%2e/health
  refused_path "prefix boundary /v1/modelsX" 404 /v1/modelsX
  refused_path "method DELETE /v1/models" 405 /v1/models -X DELETE

  if ! enroll_rogue 2>"$WORK/rogue.err"; then
    fail "enroll the rogue identity from its invite: $(cat "$WORK/rogue.err")"
    return
  fi
  # pin refuses an unpinned leaf in the filter, spiffe an unlisted ID in the handshake.
  code=$(chat_body | site_call "$WORK/rogue" /v1/chat/completions --cert "$d/rogue.crt" --key "$d/rogue.key" -d @-)
  if [[ $MODE == pin && $code == 403 ]] && ! grep -qi '^x-grid-provider-site:' "$WORK/rogue.hdr"; then
    pass "enrolled identity the site does not pin: 403"
  elif [[ $MODE == spiffe ]] && tls_refused "$WORK/rogue"; then
    pass "enrolled identity outside spiffeIds: TLS alert, curl exit $(cat "$WORK/rogue.rc")"
  else
    fail "enrolled identity the site does not trust: HTTP $code, curl exit $(cat "$WORK/rogue.rc")"
  fi
}

assert_health() {
  local ctx restarts dir hits warns
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    restarts=$(kubectl --context "$ctx" get pods -A -o json | jq -r --arg a "$NS" --arg b "$ENS" '.items[]
      | select(.metadata.namespace == $a or .metadata.namespace == $b) | .metadata.name as $p
      | ((.status.initContainerStatuses // []) + (.status.containerStatuses // []))[]
      | select(.restartCount > 0) | "\($p)/\(.name)=\(.restartCount)"')
    if [[ -z $restarts ]]; then
      pass "$ctx: zero container restarts in $NS and $ENS"
    else
      fail "$ctx: container restarts: $(tr '\n' ' ' <<<"$restarts")"
    fi
    if ! snapshot_logs "$ctx" final; then
      fail "$ctx: log collection incomplete"
      continue
    fi
    dir="$ARTIFACTS/$MODE/$ctx"
    # Pingora logs refused handshakes at ERROR: gateway TCP probes always, and the
    # deliberate-refusal checks from REFUSALS_FROM on.
    hits=$(cat "$dir"/*.log | strip_ansi | grep -E ' (ERROR)|panic|fatal' \
      | awk -v from="$REFUSALS_FROM" '/^\[pod\/grid-gateway-[^\/]+\/praxis\] .*Downstream handshake error / {
          if ($0 ~ /tls handshake eof$/ || (from != "" && substr($2, 1, 19) >= from)) next
        } { print }' | sort -u || true)
    warns=$(cat "$dir"/*.log | strip_ansi | grep -E ' WARN' | sort -u || true)
    if [[ -n $warns ]]; then
      printf 'WARN [%s] %s: %s distinct WARN lines\n%s\n' "$MODE" "$ctx" "$(wc -l <<<"$warns")" "$warns"
    fi
    if [[ -z $hits ]]; then
      pass "$ctx: no ERROR, panic, or fatal in any grid container log"
    else
      fail "$ctx: error log lines:"
      printf '%s\n' "$hits"
    fi
  done
}

cmd_test() {
  local m tool
  for m in $MODES; do
    case $m in pin | spiffe) ;; *) die "MODES entry $m is not pin or spiffe" ;; esac
  done
  for tool in kubectl helm jq openssl curl; do
    command -v "$tool" >/dev/null || die "$tool is required"
  done
  MODE=setup
  plan_addresses
  for MODE in $MODES; do
    REFUSALS_FROM=""
    log "[$MODE] resetting the $NS namespace on both clusters"
    rm -rf "${ARTIFACTS:?}/$MODE"
    reset_grid
    install_grid
    assert_membership
    assert_serving
    assert_health
  done
  if ((FAILED)); then
    printf 'RESULT: FAIL (logs in %s)\n' "$ARTIFACTS"
    return 1
  fi
  printf 'RESULT: PASS (%s)\n' "$MODES"
}

case ${1:-all} in
  up)
    cmd_up
    CREATED=0
    ;;
  test) cmd_test ;;
  down) cmd_down ;;
  all)
    cmd_up
    cmd_test
    ;;
  *)
    sed -n '2,9p' "${BASH_SOURCE[0]}" >&2
    exit 2
    ;;
esac
