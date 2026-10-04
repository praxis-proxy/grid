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
#   ROTATION_LIFETIME    site certificate lifetime in seconds for the spiffe leg, which
#                       then polls signals and asserts renewal, a fork, and re-enrollment
#   NET_PREFIX          command prefix for calls to LoadBalancer addresses, e.g. under
#                       rootless podman: podman unshare --rootless-netns
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
WATCH_PID=""
MTLS_PID=""
HUBPATH_PID=""
FAILED=0
# Start of the deliberate-refusal checks; assert_health allows gateway handshake rejections after it.
REFUSALS_FROM=""
CREATED=0
MODE=""

cleanup() {
  [[ -z $PF_PID ]] || kill "$PF_PID" 2>/dev/null || true
  stop_monitors
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

# Captured, not piped: under pipefail grep -q's early exit can fail the pipeline on a match.
cluster_exists() {
  local clusters
  clusters=$(kind get clusters 2>/dev/null || true)
  grep -qx "$PREFIX-$1" <<<"$clusters"
}

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

# renews: this leg asserts renewal, which only spiffe trust does.
renews() { [[ $MODE == spiffe && -n ${ROTATION_LIFETIME:-} ]]; }

# renewal_args <chart>: a short lifetime, signals polling, and a grid-admin for this leg.
renewal_args() {
  RA=()
  renews || return 0
  case $1 in
    grid-enrollment) RA=(--set "enrollment.certLifetimeSecs=$ROTATION_LIFETIME"
      --set enrollment.gridAdmins.serviceAccount.create=true
      --set "enrollment.enrollmentAdmins.subjects[0].kind=ServiceAccount"
      --set "enrollment.enrollmentAdmins.subjects[0].name=grid-admin"
      --set "enrollment.enrollmentAdmins.subjects[0].namespace=$ENS") ;;
    # grid.signals starts the operator in poll before its GridNetwork exists; without grid.id
    # the chart turns the signals listener on only when asked.
    grid-operator) RA=(--set grid.signals=poll --set signals.enabled=true) ;;
    grid-site) RA=(--set gridNetwork.signalTransport.mode=poll) ;;
  esac
}

# images <component>: this run's image for a chart, into IMG.
images() { IMG=(--set-string "image.repository=$IMAGE_PREFIX$1" --set-string "image.tag=$IMAGE_TAG"); }

# lb_curl: curl to a LoadBalancer address, through NET_PREFIX when set.
lb_curl() {
  # Word splitting is the point: the prefix is a command and its flags.
  # shellcheck disable=SC2086
  ${NET_PREFIX:-} curl "$@"
}

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
  renewal_args grid-enrollment
  helm_on "$HUB_CTX" "$ENS" grid-enrollment grid-enrollment "${RA[@]}" \
    --set host="$ENROLL_HOST" --set invites.hub.network=grid --set "invites.$SITE.network=grid" \
    "${IMG[@]}" --set enrollment.service.loadBalancerIP="$ENROLL_IP" --set invites.rogue.network=grid \
    || die "install grid-enrollment"
  images operator
  renewal_args grid-operator
  helm_on "$HUB_CTX" "$NS" grid-operator grid-operator "${RA[@]}" \
    --set swim.siteName=hub --set enrollment.enabled=true --set grid.peerTrust="$MODE" \
    "${IMG[@]}" --set swim.service.loadBalancerIP="$HUB_SWIM_IP" || die "install hub grid-operator"
  copy_key "$HUB_CTX" "$ENS" "$HUB_CTX" grid-ca-bundle ca.crt || die "copy the hub CA bundle"
  copy_key "$HUB_CTX" "$ENS" "$HUB_CTX" grid-invite-hub token hub || die "copy the hub invite"
  head -c 32 /dev/urandom | k "$HUB_CTX" create secret generic grid-swim-key --from-file=key=/dev/stdin >/dev/null \
    || die "create grid-swim-key"
  renewal_args grid-site
  HUB_SITE_ARGS=("${RA[@]}" --set gridNetwork.gridId="$PREFIX-e2e" --set gridSite.name=hub
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
  renewal_args grid-operator
  helm_on "$SITE_CTX" "$NS" grid-operator grid-operator "${RA[@]}" \
    --set "swim.siteName=$SITE" --set "swim.seeds=$HUB_SWIM_IP:7946" \
    --set enrollment.enabled=true --set "enrollment.url=https://grid-enrollment.$ENS.svc:$ENROLL_PORT" \
    --set grid.peerTrust="$MODE" \
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
  renewal_args grid-site
  helm_on "$SITE_CTX" "$NS" grid-site grid-site "${RA[@]}" \
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
  start_leaf_watch
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
  local phase
  phase=$(k "$1" get gridsite "$2" -o jsonpath='{.status.phase}') || return 1
  grep -qxE "$3" <<<"$phase"
}

overlay_distributed() {
  k "$HUB_CTX" get gridnetwork grid -o json | jq -e '[.status.overlayStatus[]?
    | select(.gatewayName == "grid-gateway" and .phase == "Distributed" and .candidateCount >= 1)] | length == 1'
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
}

# site_call <out prefix> <path> [curl args...]: direct TLS call to the site gateway, the
# path sent as is. Prints the HTTP status and leaves curl's exit code in <out>.rc.
site_call() {
  local out=$1 path=$2 rc=0
  shift 2
  lb_curl -sS --path-as-is -o "$out.body" -D "$out.hdr" -w '%{http_code}' --max-time 15 \
    --resolve "$SITE.grid.internal:8080:$SITE_GW_IP" --cacert "$WORK/id/ca.crt" \
    -H 'Content-Type: application/json' "$@" "https://$SITE.grid.internal:8080$path" 2>"$out.err" || rc=$?
  echo "$rc" >"$out.rc"
}

# tls_refused <out prefix> <http code>: no HTTP response, and the handshake failed with a TLS
# alert in curl's stderr (exit 35, 55 or 56).
tls_refused() {
  [[ $2 == 000 ]] || return 1
  case $(cat "$1.rc") in
    35 | 55 | 56) grep -qiE 'alert|certificate required' "$1.err" ;;
    *) return 1 ;;
  esac
}

# refused_handshake <out prefix> <http code> [curl args...]: tls_refused, or a send failure
# (exit 55) with no alert, where the same identity calling again with no body must be refused.
refused_handshake() {
  local out=$1 code=$2
  shift 2
  tls_refused "$out" "$code" && return 0
  [[ $code == 000 && $(cat "$out.rc") == 55 ]] || return 1
  code=$(site_call "$out.nobody" /v1/models "$@")
  tls_refused "$out.nobody" "$code"
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
    | lb_curl -sS --fail-with-body --resolve "$ENROLL_HOST:$ENROLL_PORT:$ENROLL_IP" --cacert "$d/ca.crt" \
      -H @<(printf 'Authorization: Bearer %s\n' "$token") -H 'Content-Type: application/json' -d @- \
      "https://$ENROLL_HOST:$ENROLL_PORT/v1alpha1/enrollments" | jq -er .certificate >"$d/rogue.crt"
}

consumer_served() {
  local out="$WORK/consumer"
  # A gateway rollout ends the port-forward's pod, so start another.
  if [[ -z $PF_PID ]] || ! kill -0 "$PF_PID" 2>/dev/null; then
    k "$HUB_CTX" port-forward service/grid-gateway 18080:8080 >"$WORK/pf.log" 2>&1 &
    PF_PID=$!
    sleep 2
  fi
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
  if refused_handshake "$WORK/nocert" "$code"; then
    pass "anonymous caller: TLS alert, curl exit $(cat "$WORK/nocert.rc") ($(tr '\n' ' ' <"$WORK/nocert.err" | cut -c1-100))"
  else
    fail "anonymous caller: HTTP $code, curl exit $(cat "$WORK/nocert.rc"), want a TLS alert ($(tr '\n' ' ' <"$WORK/nocert.err" | cut -c1-200))"
  fi

  # The backend serves /health, so a 404 here is the gateway refusing it.
  refused_path "disallowed path /health" 404 /health
  # praxis rejects dot-dot segments with 400 before routing.
  refused_path "traversal /v1/models/../../health" 400 /v1/models/../../health
  refused_path "encoded traversal /v1/models/%2e%2e/%2e%2e/health" 400 /v1/models/%2e%2e/%2e%2e/health
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
  elif [[ $MODE == spiffe ]] && refused_handshake "$WORK/rogue" "$code" --cert "$d/rogue.crt" --key "$d/rogue.key"; then
    pass "enrolled identity outside spiffeIds: TLS alert, curl exit $(cat "$WORK/rogue.rc")"
  else
    fail "enrolled identity the site does not trust: HTTP $code, curl exit $(cat "$WORK/rogue.rc") ($(tr '\n' ' ' <"$WORK/rogue.err" | cut -c1-200))"
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

# ---------------------------------------------------------------------------
# Certificate rotation
# ---------------------------------------------------------------------------

# leaf_of <context> <dir>: the identity's certificate and key, and its notBefore in epoch seconds.
leaf_of() {
  k "$1" get secret grid-site-identity -o jsonpath='{.data.tls\.crt}' | base64 -d >"$2/tls.crt" || return 1
  k "$1" get secret grid-site-identity -o jsonpath='{.data.tls\.key}' | base64 -d >"$2/tls.key" || return 1
  date -d "$(openssl x509 -in "$2/tls.crt" -noout -startdate | cut -d= -f2)" +%s
}

# watch_leaves: record every identity generation per site, certificate and key, every 2s.
watch_leaves() {
  local ctx name nb last gen tmp
  declare -A seen
  while :; do
    for ctx in "$HUB_CTX" "$SITE_CTX"; do
      name=hub
      [[ $ctx == "$HUB_CTX" ]] || name=$SITE
      tmp="$WORK/leaves/$name/tmp"
      mkdir -p "$tmp"
      nb=$(leaf_of "$ctx" "$tmp" 2>/dev/null) || continue
      last=${seen[$name]:-}
      if [[ $nb != "$last" ]]; then
        gen=$(find "$WORK/leaves/$name" -mindepth 1 -maxdepth 1 -name 'g*' | wc -l)
        mkdir -p "$WORK/leaves/$name/g$gen"
        cp "$tmp/tls.crt" "$tmp/tls.key" "$WORK/leaves/$name/g$gen/"
        echo "$nb" >"$WORK/leaves/$name/g$gen/nb"
        seen[$name]=$nb
      fi
    done
    sleep 2
  done
}

# generations <site>: identity generations seen.
generations() { find "$WORK/leaves/$1" -mindepth 1 -maxdepth 1 -name 'g*' 2>/dev/null | wc -l; }

# watch_mtls: every 5s, the hub's current leaf calls the site gateway over mutual TLS.
# A call counts as failed only after three tries, so a gateway rollout's handover is not
# an outage.
watch_mtls() {
  local gen out="$WORK/mtls-call" code try
  while :; do
    gen=$(($(generations hub) - 1))
    if ((gen >= 0)); then
      for try in 1 2 3; do
        code=$(chat_body | site_call "$out" /v1/chat/completions -d @- \
          --cert "$WORK/leaves/hub/g$gen/tls.crt" --key "$WORK/leaves/hub/g$gen/tls.key")
        [[ $code == 200 ]] && break
        sleep 2
      done
      echo "$(date -u +%H:%M:%S) hub g$gen $code $try" >>"$WORK/mtls.log"
    fi
    sleep 5
  done
}

# watch_hub_path: every 5s, a chat completion through the hub consumer gateway, which
# calls the site gateway with the hub's client certificate. Three tries, as above.
watch_hub_path() {
  local pf="" code try out="$WORK/hubpath"
  # stop_monitors kills this subshell; take its port-forward with it.
  trap '[[ -z $pf ]] || kill "$pf" 2>/dev/null; exit 0' TERM
  trap '[[ -z $pf ]] || kill "$pf" 2>/dev/null' EXIT
  while :; do
    for try in 1 2 3; do
      if [[ -z $pf ]] || ! kill -0 "$pf" 2>/dev/null; then
        k "$HUB_CTX" port-forward service/grid-gateway 18081:8080 >/dev/null 2>&1 &
        pf=$!
        sleep 2
      fi
      code=$(chat_body | curl -sS -o "$out.body" -D "$out.hdr" -w '%{http_code}' --max-time 15 \
        -H 'Content-Type: application/json' -d @- http://127.0.0.1:18081/v1/chat/completions 2>/dev/null) || true
      [[ $code == 200 ]] && served "$out" && break
      kill "$pf" 2>/dev/null || true
      pf=""
      sleep 2
    done
    echo "$(date -u +%H:%M:%S) $code $try" >>"$WORK/hubpath.log"
    sleep 5
  done
}

stop_monitors() {
  local pid
  for pid in "$WATCH_PID" "$MTLS_PID" "$HUBPATH_PID"; do
    [[ -z $pid ]] || kill "$pid" 2>/dev/null || true
  done
  WATCH_PID=""
  MTLS_PID=""
  HUBPATH_PID=""
}

# monitor_result <log> <label>: every call answered 200 within its tries.
monitor_result() {
  local calls bad
  calls=$(wc -l <"$1")
  bad=$(awk '$(NF - 1) != 200' "$1" | wc -l)
  if ((calls > 0 && bad == 0)); then
    pass "$2 answered 200 across every rotation ($calls calls)"
  else
    fail "$2 across rotations: $bad of $calls calls not 200"
    awk '$(NF - 1) != 200 && ++shown <= 5' "$1"
  fi
}

# gateway_rolled_to <context> <site>: the gateway pods carry the site's current leaf.
gateway_rolled_to() {
  local want got
  want=$(openssl x509 -in "$WORK/leaves/$2/g$(($(generations "$2") - 1))/tls.crt" -outform DER \
    | openssl dgst -sha256 -r | cut -d' ' -f1)
  got=$(k "$1" get deployment grid-gateway \
    -o jsonpath='{.spec.template.metadata.annotations.grid\.praxis\.fast/site-identity-fingerprint}')
  [[ $(tr -d ':' <<<"${got,,}") == "$want" ]]
}

# poll_counts <context> <port>: this operator's peer polls, as outcome=count lines. The
# enrolled operator serves metrics over TLS from its site identity.
poll_counts() {
  local pid rc=0 name=hub
  [[ $1 == "$HUB_CTX" ]] || name=$SITE
  k "$HUB_CTX" get secret grid-ca -o jsonpath='{.data.ca\.crt}' | base64 -d >"$WORK/grid-ca.crt" || return 1
  k "$1" port-forward deployment/grid-operator "$2:9090" >/dev/null 2>&1 &
  pid=$!
  sleep 2
  curl -sS --max-time 5 --cacert "$WORK/grid-ca.crt" --resolve "$name.grid.internal:$2:127.0.0.1" \
    "https://$name.grid.internal:$2/metrics" \
    | awk -F'[{}]' '/^grid_peer_poll_total\{/ { split($2, l, ","); for (i in l) if (l[i] ~ /^outcome=/) { o = l[i]; gsub(/outcome=|"/, "", o) } ; n[o] += $3 } END { for (o in n) print o "=" n[o] }' \
    | sort || rc=1
  kill "$pid" 2>/dev/null || true
  return "$rc"
}

# renewals_logged <context>: INFO "site identity rotated" lines across this operator's pods.
renewals_logged() {
  k "$1" logs deployment/grid-operator --all-containers 2>/dev/null | strip_ansi \
    | grep -c ' INFO .*site identity rotated' || true
}

# enroll_api <out> <method> <path> [curl args...]: a call to the enrollment service.
enroll_api() {
  local out=$1 method=$2 path=$3
  shift 3
  lb_curl -sS -o "$out.body" -w '%{http_code}' --max-time 15 -X "$method" \
    --resolve "$ENROLL_HOST:$ENROLL_PORT:$ENROLL_IP" --cacert "$WORK/id/ca.crt" \
    -H 'Content-Type: application/json' "$@" "https://$ENROLL_HOST:$ENROLL_PORT/v1alpha1$path" 2>"$out.err"
}

# renew_with <out> <cert> <key>: ask for a new key's leaf with the given identity.
renew_with() {
  local out=$1 key="$WORK/fork.key"
  openssl ecparam -name prime256v1 -genkey -noout -out "$key" 2>/dev/null
  openssl req -new -key "$key" -subj "/CN=$SITE" -out "$WORK/fork.csr" 2>/dev/null
  jq -n --rawfile csr "$WORK/fork.csr" '{csr: $csr}' | enroll_api "$out" POST /rotations -d @- --cert "$2" --key "$3"
}

admin_token() {
  kubectl --context "$HUB_CTX" -n "$ENS" create token grid-admin --audience grid-enrollment --duration 10m
}

# start_leaf_watch: before install, so the first identity of each site is generation 0.
start_leaf_watch() {
  renews || return 0
  rm -rf "${WORK:?}/leaves"
  mkdir -p "$WORK/leaves/hub" "$WORK/leaves/$SITE"
  : >"$WORK/mtls.log"
  : >"$WORK/hubpath.log"
  watch_leaves &
  WATCH_PID=$!
}

# polled <context> <port>: this operator has polled its peer successfully at least once.
polled() { (($(ok_count "$(poll_counts "$1" "$2" | tr '\n' ' ')") > 0)); }

# poll_baseline <context> <port>: poll counts from a scrape that succeeded and returned
# lines, retried a few times, so a failed scrape is never read as zero polls.
poll_baseline() {
  local out
  for _ in 1 2 3 4 5; do
    if out=$(poll_counts "$1" "$2") && [[ -n $out ]]; then
      tr '\n' ' ' <<<"$out"
      return 0
    fi
    sleep 2
  done
  return 1
}

# start_renewal_watch: the poll baseline, once each site has polled its peer, so a peer
# not yet serving at install is not counted against the rotations.
start_renewal_watch() {
  renews || return 0
  eventually "hub polled its peer's signals" polled "$HUB_CTX" 19090 || true
  eventually "$SITE polled its peer's signals" polled "$SITE_CTX" 19091 || true
  POLLS_HUB_START=$(poll_baseline "$HUB_CTX" 19090) || fail "hub poll baseline: metrics scrape failed"
  POLLS_SITE_START=$(poll_baseline "$SITE_CTX" 19091) || fail "$SITE poll baseline: metrics scrape failed"
}

# at_least_renewed <n>: both sites have n identities beyond their first.
at_least_renewed() { (($(generations hub) > $1 && $(generations "$SITE") > $1)); }

# not_before_advances <site>: each generation starts after the one before.
not_before_advances() {
  local g prev=0 nb
  for ((g = 0; g < $(generations "$1"); g++)); do
    nb=$(cat "$WORK/leaves/$1/g$g/nb")
    ((nb > prev)) || return 1
    prev=$nb
  done
}

# fail_count <counts>: polls whose outcome is not ok.
fail_count() { tr ' ' '\n' <<<"$1" | awk -F= '$1 != "ok" && $1 != "" { n += $2 } END { print n + 0 }'; }
ok_count() { tr ' ' '\n' <<<"$1" | awk -F= '$1 == "ok" { n += $2 } END { print n + 0 }'; }

assert_renewal() {
  local deadline name ctx logged gens calls bad end_hub end_site out="$WORK/renew" prev cur g code token
  renews || return 0
  watch_mtls &
  MTLS_PID=$!
  watch_hub_path &
  HUBPATH_PID=$!
  # A renewal is due at two thirds of the lifetime plus the 5 minute backdate; allow three.
  deadline=$((SECONDS + 3 * (ROTATION_LIFETIME + 300) * 2 / 3 + 120))
  until at_least_renewed 2; do
    if ((SECONDS >= deadline)); then
      fail "hub and $SITE each renewed twice (hub $(generations hub), $SITE $(generations "$SITE") generations)"
      stop_monitors
      return
    fi
    sleep 5
  done
  pass "hub and $SITE each renewed at least twice (hub $(generations hub), $SITE $(generations "$SITE") generations)"
  for name in hub "$SITE"; do
    if not_before_advances "$name"; then
      pass "$name notBefore advances with every renewal"
    else
      fail "$name notBefore did not advance: $(cat "$WORK"/leaves/"$name"/g*/nb | tr '\n' ' ')"
    fi
  done
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    name=hub
    [[ $ctx == "$HUB_CTX" ]] || name=$SITE
    logged=$(renewals_logged "$ctx")
    gens=$(($(generations "$name") - 1))
    if ((logged == gens)); then
      pass "$name operator logged one INFO per renewal ($logged)"
    else
      fail "$name operator logged $logged renewals, saw $gens"
    fi
  done

  kill "$MTLS_PID" "$HUBPATH_PID" 2>/dev/null || true
  MTLS_PID=""
  HUBPATH_PID=""
  monitor_result "$WORK/mtls.log" "site gateway, called with the hub's current leaf over mutual TLS,"
  monitor_result "$WORK/hubpath.log" "hub consumer gateway to the site gateway"
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    name=hub
    [[ $ctx == "$HUB_CTX" ]] || name=$SITE
    eventually "$name gateway Deployment rolled onto its current leaf" gateway_rolled_to "$ctx" "$name" || true
  done
  end_hub=$(poll_baseline "$HUB_CTX" 19090) || fail "hub poll counts: metrics scrape failed"
  end_site=$(poll_baseline "$SITE_CTX" 19091) || fail "$SITE poll counts: metrics scrape failed"
  for name in hub "$SITE"; do
    if [[ $name == hub ]]; then prev=$POLLS_HUB_START cur=$end_hub; else prev=$POLLS_SITE_START cur=$end_site; fi
    if (($(ok_count "$cur") > $(ok_count "$prev") && $(fail_count "$cur") == $(fail_count "$prev"))); then
      pass "$name polled its peer's signals across every rotation with no failed poll ($(($(ok_count "$cur") - $(ok_count "$prev"))) ok)"
    else
      fail "$name peer polls: start [$prev] end [$cur]"
    fi
  done

  # A replaced leaf asking for a new key is a fork: refused, and the site freezes.
  g=$(($(generations "$SITE") - 2))
  until openssl x509 -in "$WORK/leaves/$SITE/g$g/tls.crt" -noout -checkend 30 >/dev/null; do
    if ((SECONDS >= deadline + ROTATION_LIFETIME)); then
      fail "no replaced $SITE leaf still valid for the fork check"
      stop_monitors
      return
    fi
    sleep 5
    g=$(($(generations "$SITE") - 2))
  done
  stop_monitors
  code=$(renew_with "$out" "$WORK/leaves/$SITE/g$g/tls.crt" "$WORK/leaves/$SITE/g$g/tls.key")
  if [[ $code == 403 ]] && jq -e '.error == "identity_refused"' "$out.body" >/dev/null 2>&1; then
    pass "a replaced $SITE leaf asking for a new key: 403 identity_refused"
  else
    fail "replaced leaf renewal: HTTP $code $(head -c 200 "$out.body" 2>/dev/null)"
  fi
  g=$(($(generations "$SITE") - 1))
  code=$(renew_with "$out" "$WORK/leaves/$SITE/g$g/tls.crt" "$WORK/leaves/$SITE/g$g/tls.key")
  if [[ $code == 403 ]]; then
    pass "the fork froze $SITE: even its current leaf cannot renew: 403"
  else
    fail "frozen site renewal with its current leaf: HTTP $code"
  fi

  # A grid-admin deletes the enrollment, mints an invite, and the site enrolls again.
  token=$(admin_token) || { fail "mint a grid-admin token"; return; }
  code=$(enroll_api "$out" DELETE "/enrollments/$SITE" -H @<(printf 'Authorization: Bearer %s\n' "$token"))
  if [[ $code == 204 ]]; then
    pass "grid-admin deleted the $SITE enrollment: 204"
  else
    fail "delete the $SITE enrollment: HTTP $code $(head -c 200 "$out.body" 2>/dev/null)"
    return
  fi
  code=$(jq -cn --arg s "$SITE" '{siteName: $s, gridNetworkRef: "grid"}' \
    | enroll_api "$out" POST /enrollmenttokens -d @- -H @<(printf 'Authorization: Bearer %s\n' "$token"))
  if [[ $code != 201 ]] || ! jq -er .token "$out.body" >"$WORK/invite" 2>/dev/null; then
    fail "mint a new $SITE invite: HTTP $code $(head -c 200 "$out.body" 2>/dev/null)"
    return
  fi
  k "$SITE_CTX" delete secret "grid-invite-$SITE" grid-site-identity --ignore-not-found >/dev/null
  k "$SITE_CTX" create secret generic "grid-invite-$SITE" --from-file=token="$WORK/invite" >/dev/null
  k "$SITE_CTX" label secret "grid-invite-$SITE" "grid.praxis.fast/site=$SITE" >/dev/null
  k "$SITE_CTX" rollout restart deployment/grid-operator >/dev/null
  rm -rf "${WORK:?}/leaves/$SITE"
  mkdir -p "$WORK/leaves/$SITE"
  watch_leaves &
  WATCH_PID=$!
  eventually "$SITE re-enrolled with a new invite after the delete" enrolled "$SITE_CTX" || { stop_monitors; return; }
  deadline=$((SECONDS + (ROTATION_LIFETIME + 300) * 2 / 3 + 180))
  until (($(generations "$SITE") > 1)); do
    if ((SECONDS >= deadline)); then
      fail "re-enrolled $SITE renewed (generations $(generations "$SITE"))"
      stop_monitors
      return
    fi
    sleep 5
  done
  stop_monitors
  pass "re-enrolled $SITE renews again"
}

# Pin trust does not renew: the operator says so once and schedules no renewal.
assert_pin_no_renewal() {
  local ctx logs
  [[ $MODE == pin ]] || return 0
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    # Captured first: grep -q exits at its match, and pipefail would report the writer's SIGPIPE.
    logs=$(k "$ctx" logs deployment/grid-operator --all-containers | strip_ansi)
    if grep -q 'rotation disabled: peerTrust pin' <<<"$logs"; then
      pass "$ctx: operator logged that pin trust disables renewal"
    else
      fail "$ctx: no rotation disabled line under pin trust"
    fi
    if [[ -z $(k "$ctx" get gridnetwork grid -o jsonpath='{.status.identity.rotateAfter}') ]]; then
      pass "$ctx: status.identity.rotateAfter is empty under pin trust"
    else
      fail "$ctx: status.identity.rotateAfter set under pin trust"
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
    start_renewal_watch
    assert_membership
    assert_serving
    assert_pin_no_renewal
    assert_renewal
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
