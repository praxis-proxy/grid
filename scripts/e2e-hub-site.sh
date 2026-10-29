#!/usr/bin/env bash
# Hub and site install e2e: forge creates a hub and a site kind cluster, then the script
# runs the examples/helm/hub-site README commands, with test-only overrides, and asserts
# enrollment, membership, serving, peer trust, and log health per trust mode.
#
# Usage: e2e-hub-site.sh [all|up|test|down|selftest]   (default all)
#   up     build and load the images, create the clusters, apply the topology stacks
#   test   per mode in MODES: reset the grid namespace, install, assert
#   down   delete the clusters
#   selftest  check the HA verdicts against synthetic monitor logs, no cluster
#
# Env:
#   MODES               legs to run (default "pin spiffe"): the peer trust modes, and
#                       local, which is spiffe trust under Local membership, with signals
#                       polled where the GridSites say and no SWIM seed between the sites
#   HUB_ROUTER          hub gateway config: render (default), or operator, which serves the
#                       operator's generated gridSiteRoute config under poll signals
#   IMAGE_PREFIX        image repository prefix (default localhost/grid-)
#   IMAGE_TAG           image tag (default e2e)
#   SKIP_BUILD=1        use prebuilt <prefix>{operator,gateway,enrollment}:<tag> images
#   GRID_FEEDBACK_CONTRACT_TEST=1  run the real API/TLS contract before install (CI pin leg)
#   CONTAINER_TOOL      image build tool (default docker)
#   FORGE_BIN           explicit executable override (default: pinned upstream Forge)
#   KIND_CREATE_PREFIX  command prefix for cluster creation, e.g. the rootless podman
#                       wrapper: systemd-run --scope --user -p Delegate=yes
#   KEEP=1              keep clusters this run created
#   ARTIFACTS           log directory (default /tmp/grid-hub-site-e2e)
#   TIMEOUT             seconds to wait for each converging condition (default 180)
#   ROTATION_LIFETIME    site certificate lifetime in seconds for the spiffe leg, which
#                       then polls signals and asserts renewal, a fork, and re-enrollment
#   SIGNED_STATE        peerTrust.signedState, off or enforce (default off). enforce also
#                       asserts that the hub refuses the site's state once it stops signing
#   NET_PREFIX          command prefix for calls to LoadBalancer addresses, e.g. under
#                       rootless podman: podman unshare --rootless-netns
#   TOPOLOGY            topology under tests/e2e/topologies (default grid-hub-site, or
#                       grid-hub-site-ha under HA=1)
#   HA=1                run the HA fault scenarios: steady load through the hub gateway's
#                       LoadBalancer while the gateway and the operator are restarted,
#                       deleted and drained. Needs a hub with two worker nodes.
#   HA_GATEWAY_REPLICAS  hub gateway replicas under HA=1 (default 2)
#   HA_SIGNALS          signal transport for the HA leg: gossip (default) or poll. Only poll
#                       runs a signals listener, so only poll yields a signals handover number
#   HA_OPERATOR_NODE    node to pin the hub operator to (default: the first worker)
#   HA_OPERATOR_REPLICAS operator replicas under HA=1 (default 2 where the chart accepts it,
#                       else 1)
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# The HA leg needs worker nodes on the hub, so it has a topology of its own.
TOPOLOGY=${TOPOLOGY:-$([[ ${HA:-0} == 1 ]] && echo grid-hub-site-ha || echo grid-hub-site)}
TOPO="$ROOT/tests/e2e/topologies/$TOPOLOGY"
MODES=${MODES:-pin spiffe}
HUB_ROUTER=${HUB_ROUTER:-render}
SIGNED_STATE=${SIGNED_STATE:-off}
IMAGE_PREFIX=${IMAGE_PREFIX:-localhost/grid-}
IMAGE_TAG=${IMAGE_TAG:-e2e}
CONTAINER_TOOL=${CONTAINER_TOOL:-docker}
ARTIFACTS=${ARTIFACTS:-/tmp/grid-hub-site-e2e}
TIMEOUT=${TIMEOUT:-180}
HA=${HA:-0}
HA_GATEWAY_REPLICAS=${HA_GATEWAY_REPLICAS:-2}
HA_OPERATOR_REPLICAS=${HA_OPERATOR_REPLICAS:-}
PREFIX=$TOPOLOGY
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
CORDONED=""
FRONTDOOR_PID=""
CATALOG_PID=""
DRAIN_PID=""

# When signed state was turned on, for the signing log check.
SIGNED_FROM=""

FAILED=0
# Start of the deliberate-refusal checks; assert_health allows gateway handshake rejections after it.
REFUSALS_FROM=""
CREATED=0
MODE=""
TRUST=""

cleanup() {
  [[ -z $PF_PID ]] || kill "$PF_PID" 2>/dev/null || true
  uncordon_node
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
  FORGE_BIN=$("$ROOT/scripts/forge.sh" path)
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
  feedback_contract_test
}

# The CI-only test owns exactly one object and the CRD it installs on the empty hub.
feedback_contract_test() (
  [[ ${GRID_FEEDBACK_CONTRACT_TEST:-0} == 1 ]] || return 0
  local crd=gridnetworks.grid.praxis.fast existing
  existing=$(kubectl --context "$HUB_CTX" get crd "$crd" --ignore-not-found -o name) \
    || die "cannot inspect the isolated hub CRDs"
  [[ -z $existing ]] || die "feedback contract requires a hub without the GridNetwork CRD"
  kubectl --context "$HUB_CTX" create -f "$ROOT/deploy/crds/gridnetwork.yaml" \
    || die "cannot create the isolated feedback CRD"
  trap 'contract_rc=$?
        kubectl --context "$HUB_CTX" delete gridnetwork gateway-feedback-contract --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || contract_rc=1
        kubectl --context "$HUB_CTX" delete crd gridnetworks.grid.praxis.fast --ignore-not-found --wait=true --timeout=60s >/dev/null 2>&1 || contract_rc=1
        exit "$contract_rc"' EXIT
  kubectl --context "$HUB_CTX" wait --for=condition=Established "crd/$crd" --timeout=60s
  mkdir -p "$ARTIFACTS"
  GRID_FEEDBACK_TEST_CONTEXT="$HUB_CTX" cargo test -p operator --bin operator \
    tests::feedback_contract::tls_feedback_persists_against_real_kubernetes -- --ignored --exact \
    2>&1 | tee "$ARTIFACTS/gateway-feedback-contract.log"
)

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
  # Signals have their own Service, so their own address from each pool.
  HUB_SIG_IP=$(ip_add "$hub" 3)
  # The HA leg puts the hub gateway behind a LoadBalancer of its own; reserved either way,
  # so the address map does not change shape between legs.
  HUB_GW_IP=$(ip_add "$hub" 4)
  SITE_SWIM_IP=$(ip_add "$site" 1)
  SITE_GW_IP=$(ip_add "$site" 2)
  SITE_SIG_IP=$(ip_add "$site" 3)
  MODEL_LB_IP=$(ip_add "$site" 4)
  HUB_GW_IP=$(ip_add "$hub" 4)
  MODEL_IP=$(kubectl --context "$SITE_CTX" -n model get service vcr-inference-site -o jsonpath='{.spec.clusterIP}')
  [[ -n $MODEL_IP ]] || die "no vcr-inference-site Service on the site"
  # Two Services asking MetalLB for one address leaves the loser pending forever,
  # which surfaces as an unrelated deployment never going ready.
  local planned
  planned=$(printf '%s\n' "$ENROLL_IP" "$HUB_SWIM_IP" "$HUB_SIG_IP" "$HUB_GW_IP" \
    "$SITE_SWIM_IP" "$SITE_SIG_IP" "$SITE_GW_IP" "$MODEL_LB_IP")
  [[ $(printf '%s\n' "$planned" | sort -u | wc -l) -eq 8 ]] \
    || die "planned addresses are not distinct: $(printf '%s ' "$planned")"
  log "enrollment $ENROLL_IP, hub SWIM $HUB_SWIM_IP, hub signals $HUB_SIG_IP, hub gateway $HUB_GW_IP"
  log "site SWIM $SITE_SWIM_IP, site signals $SITE_SIG_IP, site gateway $SITE_GW_IP"
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

# site_route: the hub gateway serves the operator's generated gridSiteRoute config.
site_route() { [[ $HUB_ROUTER == operator ]]; }

# local_source: this leg runs Local membership, polling signals where the GridSites say, with
# no SWIM seed between the sites.
local_source() { [[ $MODE == local ]]; }

# polls: this leg runs signals in poll.
polls() { renews || local_source || site_route; }

# renewal_args <chart>: a short lifetime and a grid-admin for the renewal leg, and signals
# polling for it, the Local membership leg and the site route leg.
renewal_args() {
  RA=()
  if renews && [[ $1 == grid-enrollment ]]; then
    RA=(--set "enrollment.certLifetimeSecs=$ROTATION_LIFETIME"
      --set enrollment.gridAdmins.serviceAccount.create=true
      --set "enrollment.enrollmentAdmins.subjects[0].kind=ServiceAccount"
      --set "enrollment.enrollmentAdmins.subjects[0].name=grid-admin"
      --set "enrollment.enrollmentAdmins.subjects[0].namespace=$ENS")
  fi
  # The HA leg serves signals, so an observer's read can be asserted, and leaves membership
  # on whatever the trust mode already uses.
  if ha && ! renews && ! site_route; then
    case $1 in
      # The listener needs asking for without grid.id, and the transport decides whether
      # anything runs behind it, so a signals number needs HA_SIGNALS=poll.
      grid-operator) RA=(--set signals.enabled=true) ;;
      grid-site) [[ ${HA_SIGNALS:-gossip} != poll ]] || RA=(--set gridNetwork.signalTransport.mode=poll) ;;
    esac
    return 0
  fi
  ! local_source || RA+=(--set grid.membership.source=Local)
  ! local_source || [[ $1 != grid-operator ]] || RA+=(--set signals.service.type=LoadBalancer)
  polls || return 0
  case $1 in
    # grid.signals starts the operator in poll before its GridNetwork exists; without grid.id
    # the chart turns the signals listener on only when asked.
    grid-operator) RA+=(--set grid.signals=poll --set signals.enabled=true) ;;
    grid-site) RA+=(--set gridNetwork.signalTransport.mode=poll) ;;
  esac
}

# Test fixture: the site route leg's hub-local provider dials the site's model, which answers
# only on a ClusterIP, through a LoadBalancer of its own.
model_lb() {
  kubectl --context "$SITE_CTX" -n model get service vcr-inference-site -o json \
    | jq --arg ip "$MODEL_LB_IP" '{apiVersion: "v1", kind: "Service",
        metadata: {name: "vcr-inference-hub-local", namespace: "model"},
        spec: {type: "LoadBalancer", loadBalancerIP: $ip, selector: .spec.selector,
          ports: [.spec.ports[] | {name, port, targetPort, protocol}]}}' \
    | kubectl --context "$SITE_CTX" apply -f - >/dev/null || return 1
  # The model admits only its own site's grid namespace; the hub reaches it over the kind network.
  kubectl --context "$SITE_CTX" apply -f - >/dev/null <<EOF
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: vcr-inference-allow-hub-local
  namespace: model
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: vllm-vcr
  policyTypes: [Ingress]
  ingress:
    - from:
        - ipBlock:
            cidr: ${MODEL_LB_IP%.*.*}.0.0/16
      ports:
        - protocol: TCP
          port: 8000
EOF
}

model_lb_ready() {
  [[ $(kubectl --context "$SITE_CTX" -n model get service vcr-inference-hub-local \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}') == "$MODEL_LB_IP" ]]
}

# site_route_providers <providers...>: the hub GridNetwork's generated gridSiteRoute config,
# derived from these providers, and the hub-local provider beside the site's.
site_route_providers() {
  local i=0 provider
  SR=(--set "gridNetwork.consumerGateways[0].praxisConfig.generate=true"
    --set "gridNetwork.consumerGateways[0].praxisConfig.siteRouter=gridSiteRoute"
    --set "gridNetwork.consumerGateways[0].praxisConfig.mountReconciliation.enabled=true"
    --set "gridNetwork.consumerGateways[0].praxisConfig.mountReconciliation.deploymentName=grid-gateway"
    --set "inferenceProviders.hub-local.endpoint=http://$MODEL_LB_IP:8000"
    --set "inferenceProviders.hub-local.model=$MODEL")
  for provider in "$@"; do
    SR+=(--set "gridNetwork.consumerGateways[0].praxisConfig.deriveTopology.fromProviders[$i]=$provider")
    i=$((i + 1))
  done
}

# signed_args: under enforce, signedState and the identity Secrets that sign, into SA.
# Applied only after enrollment: naming absent Secrets makes the operator self-sign them,
# and enrollment then finds an identity and never runs.
signed_args() {
  SA=()
  [[ $SIGNED_STATE == enforce ]] || return 0
  SA=(--set gridNetwork.peerTrust.signedState=enforce
    --set gridNetwork.tls.siteSecretRef.name=grid-site-identity --set "gridNetwork.tls.siteSecretRef.namespace=$NS"
    --set gridNetwork.tls.caSecretRef.name=grid-ca --set "gridNetwork.tls.caSecretRef.namespace=$NS")
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

# network_condition <context> <type>: status/reason of one GridNetwork condition. This
# site's own conditions live there; its GridSite carries membership only.
network_condition() {
  kubectl --context "$1" get gridnetwork grid \
    -o jsonpath="{.status.conditions[?(@.type==\"$2\")].status}/{.status.conditions[?(@.type==\"$2\")].reason}"
}

# site_reports_enrolled <context>: the signer issued this site's identity, so a Secret some
# other writer left cannot pass as enrolled.
site_reports_enrolled() { [[ $(network_condition "$1" Enrolled) == True/Enrolled ]]; }

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

# promptly <what> <command...>: eventually, at one second, for a wait whose duration is
# reported. The five second poll elsewhere is right for a pass or fail and too coarse here.
promptly() {
  local what=$1 deadline=$((SECONDS + TIMEOUT))
  shift
  until "$@" >/dev/null 2>&1; do
    if ((SECONDS >= deadline)); then
      fail "$what (not within ${TIMEOUT}s)"
      return 1
    fi
    sleep 1
  done
  pass "$what"
}

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
  local -a hub_gateway_args=()
  images enrollment
  renewal_args grid-enrollment
  helm_on "$HUB_CTX" "$ENS" grid-enrollment grid-enrollment "${RA[@]}" \
    --set host="$ENROLL_HOST" --set invites.hub.network=grid --set "invites.$SITE.network=grid" \
    "${IMG[@]}" --set enrollment.service.loadBalancerIP="$ENROLL_IP" --set invites.rogue.network=grid \
    || die "install grid-enrollment"
  images operator
  renewal_args grid-operator
  ha_operator_args
  # Local membership: the site's gateway and signals share one load balancer, so the hub dials the
  # gateway host at the signals port, under <site>-signals.grid.internal, which the listener ignores.
  ! local_source || RA+=(--set signals.peerEndpointPort=9091)
  helm_on "$HUB_CTX" "$NS" grid-operator grid-operator "${RA[@]}" "${OA[@]}" \
    --set swim.siteName=hub --set enrollment.enabled=true --set grid.peerTrust="$TRUST" \
    --set "enrollment.url=https://grid-enrollment.$ENS.svc:$ENROLL_PORT" \
    "${IMG[@]}" --set swim.service.loadBalancerIP="$HUB_SWIM_IP" \
    --set signals.service.loadBalancerIP="$HUB_SIG_IP" || die "install hub grid-operator"
  copy_key "$HUB_CTX" "$ENS" "$HUB_CTX" grid-ca-bundle ca.crt || die "copy the hub CA bundle"
  k "$HUB_CTX" label secret grid-ca-bundle grid.praxis.fast/signer-bundle=true >/dev/null || die "opt the hub CA bundle in"
  copy_key "$HUB_CTX" "$ENS" "$HUB_CTX" grid-invite-hub token hub || die "copy the hub invite"
  head -c 32 /dev/urandom | k "$HUB_CTX" create secret generic grid-swim-key --from-file=key=/dev/stdin >/dev/null \
    || die "create grid-swim-key"
  renewal_args grid-site
  HUB_SITE_ARGS=("${RA[@]}" --set gridNetwork.gridId="$PREFIX-e2e" --set gridSite.name=hub
    --set "peers.$SITE.address=$SITE_GW_IP:8080"
    --set gridNetwork.peerTrust.mode="$TRUST" --set "gridNetwork.consumerGateways[0].name=grid-gateway"
    --set "gridNetwork.consumerGateways[0].namespace=$NS" --set "gridNetwork.consumerGateways[0].siteName=hub")
  SR=()
  site_route && site_route_providers vcr hub-local
  helm_on "$HUB_CTX" "$NS" grid-site grid-site "${HUB_SITE_ARGS[@]}" "${SR[@]}" || die "install hub grid-site"
  images gateway
  ha_gateway_args
  if site_route; then
    if [[ $SIGNED_STATE == enforce ]]; then
      hub_gateway_args+=(--set service.type=LoadBalancer --set "service.loadBalancerIP=$HUB_GW_IP"
        --set praxisConfig.operator.allowUnauthenticatedExposure=true)
    fi
    helm_on "$HUB_CTX" "$NS" grid-gateway praxis-gateway \
      --set fullnameOverride=grid-gateway --set praxisConfig.source=operator --set image.flavor=grid-gateway \
      --set gridIdentity.tlsSecretName=grid-site-identity --set gridIdentity.caSecretName=grid-ca \
      --set praxisConfig.operator.gridServing.enabled=true --set mountReconciliation.enabled=true \
      --set mountReconciliation.network=grid --set mountReconciliation.gatewayRef=grid-gateway \
      --set "mountReconciliation.operator.namespace=$NS" "${IMG[@]}" \
      "${hub_gateway_args[@]}" "${GA[@]}" || die "install hub grid-gateway"
  else
    if [[ $SIGNED_STATE == enforce ]]; then
      # The site probes the discovered gateway with Grid CA trust.
      hub_gateway_args+=(--set service.type=LoadBalancer --set "service.loadBalancerIP=$HUB_GW_IP"
        --set praxisConfig.render.auth.allowUnauthenticatedExposure=true
        --set listenerTls.secretName=grid-site-identity)
    fi
    helm_on "$HUB_CTX" "$NS" grid-gateway praxis-gateway \
      --set fullnameOverride=grid-gateway --set praxisConfig.source=render --set grid.siteName=hub \
      --set gridIdentity.tlsSecretName=grid-site-identity --set gridIdentity.caSecretName=grid-ca \
      --set praxisConfig.render.model="$MODEL" --set praxisConfig.render.auth.mode=none \
      --set "praxisConfig.render.backends.$SITE.endpoint=$SITE_GW_IP:8080" "${IMG[@]}" \
      "${hub_gateway_args[@]}" "${GA[@]}" || die "install hub grid-gateway"
  fi
  eventually "hub operator enrolled from its own invite through the in-cluster Service" enrolled "$HUB_CTX" \
    || die "hub not enrolled"
  eventually "hub GridNetwork reports Enrolled by the signer" site_reports_enrolled "$HUB_CTX" \
    || die "hub identity not issued by the signer"
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
  local trust seeds
  images operator
  renewal_args grid-operator
  seeds=(--set "swim.seeds=$HUB_SWIM_IP:7946")
  ! local_source || seeds=()
  local sig_ip=$SITE_SIG_IP shared=()
  # One load balancer per site: the signals Service shares the gateway's address. MetalLB
  # shares an address between Local Services only with one selector, so it is Cluster, and
  # every peer arrives from a node address, which needs the per-source cap raised.
  if local_source; then
    sig_ip=$SITE_GW_IP
    shared=(--set-string 'signals.service.annotations.metallb\.universe\.tf/allow-shared-ip=grid-site'
      --set signals.service.externalTrafficPolicy=Cluster --set signals.maxHandshakesPerSource=64)
  fi
  helm_on "$SITE_CTX" "$NS" grid-operator grid-operator "${RA[@]}" \
    --set "swim.siteName=$SITE" "${seeds[@]}" \
    --set enrollment.enabled=true \
    --set "enrollment.url=https://grid-enrollment.$ENS.svc:$ENROLL_PORT" \
    --set grid.peerTrust="$TRUST" \
    "${IMG[@]}" --set swim.service.loadBalancerIP="$SITE_SWIM_IP" \
    --set signals.service.loadBalancerIP="$sig_ip" "${shared[@]}" || die "install site grid-operator"
  enrollment_forward || die "enrollment forward"
  copy_key "$HUB_CTX" "$ENS" "$SITE_CTX" grid-ca-bundle ca.crt || die "copy the site CA bundle"
  k "$SITE_CTX" label secret grid-ca-bundle grid.praxis.fast/signer-bundle=true >/dev/null || die "opt the site CA bundle in"
  copy_key "$HUB_CTX" "$ENS" "$SITE_CTX" "grid-invite-$SITE" token "$SITE" || die "copy the site invite"
  copy_key "$HUB_CTX" "$NS" "$SITE_CTX" grid-swim-key key || die "copy the SWIM key"
  if [[ $(k "$SITE_CTX" get secret "grid-invite-$SITE" -o jsonpath='{.metadata.labels.grid\.praxis\.fast/site}') == "$SITE" ]]; then
    pass "copied grid-ca-bundle, grid-invite-$SITE (site label set), and grid-swim-key to the site"
  else
    fail "copied invite lacks its grid.praxis.fast/site label"
  fi
  renewal_args grid-site
  SITE_SITE_ARGS=("${RA[@]}" --set gridNetwork.gridId="$PREFIX-e2e" --set "gridSite.name=$SITE"
    --set "peers.hub.digest=$HUB_DIGEST" --set "inferenceProviders.vcr.endpoint=http://$MODEL_IP:8000"
    --set "inferenceProviders.vcr.model=$MODEL" --set gridNetwork.peerTrust.mode="$TRUST")
  if [[ $SIGNED_STATE == enforce ]]; then
    SITE_SITE_ARGS+=(--set "gridNetwork.consumerGateways[0].name=grid-gateway"
      --set "gridNetwork.consumerGateways[0].namespace=$NS"
      --set "gridNetwork.consumerGateways[0].siteName=$SITE")
  fi
  helm_on "$SITE_CTX" "$NS" grid-site grid-site "${SITE_SITE_ARGS[@]}" || die "install site grid-site"
  trust=(--set "praxisConfig.render.peerTrust.digest=$HUB_DIGEST")
  [[ $TRUST == pin ]] || trust=(--set praxisConfig.render.peerTrust.mode=spiffe
    --set praxisConfig.render.peerTrust.spiffeId=spiffe://grid.internal/site/hub)
  local gw_shared=()
  ! local_source || gw_shared=(--set-string 'service.annotations.metallb\.universe\.tf/allow-shared-ip=grid-site')
  images gateway
  helm_on "$SITE_CTX" "$NS" grid-gateway praxis-gateway \
    --set fullnameOverride=grid-gateway --set praxisConfig.source=render --set praxisConfig.render.role=provider \
    --set "grid.siteName=$SITE" --set gridIdentity.tlsSecretName=grid-site-identity --set gridIdentity.caSecretName=grid-ca "${trust[@]}" \
    --set "praxisConfig.render.backends.local.endpoint=$MODEL_IP:8000" \
    "${IMG[@]}" --set service.loadBalancerIP="$SITE_GW_IP" "${gw_shared[@]}" || die "install site grid-gateway"
  eventually "site operator enrolled from its copied invite through the hub enrollment URL" enrolled "$SITE_CTX" \
    || die "site not enrolled"
  eventually "$SITE GridNetwork reports Enrolled by the signer" site_reports_enrolled "$SITE_CTX" \
    || die "$SITE identity not issued by the signer"
  signed_args
  SIGNED_FROM=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  HUB_SITE_ARGS+=("${SA[@]}")
  SITE_SITE_ARGS+=("${SA[@]}")
  ((${#SA[@]} == 0)) || helm_on "$SITE_CTX" "$NS" grid-site grid-site "${SITE_SITE_ARGS[@]}" \
    || die "sign the site's state"
  # Every later hub grid-site upgrade carries the site's digest: helm does not reuse the release's values.
  HUB_SITE_ARGS+=(--set "peers.$SITE.digest=$(leaf_digest "$SITE_CTX")")
  helm_on "$HUB_CTX" "$NS" grid-site grid-site "${HUB_SITE_ARGS[@]}" "${SR[@]}" || die "pin the site on the hub"
  snapshot_logs "$SITE_CTX" site || true
}

install_grid() {
  local ctx
  start_leaf_watch
  install_hub
  install_site
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    # Name what is stuck and why. A bare timeout here cost two rounds of diagnosis:
    # the pod was retrying a forbidden read and the message said only "not ready".
    k "$ctx" rollout status deployment --timeout 5m >/dev/null && continue
    k "$ctx" get deploy,pods -o wide >&2 || true
    for p in $(k "$ctx" get pods -o name 2>/dev/null); do
      k "$ctx" get "$p" -o jsonpath='{.metadata.name}{"\t"}{range .status.conditions[?(@.type=="Ready")]}{.status}{end}{"\n"}' >&2 2>/dev/null || true
      k "$ctx" logs "$p" --tail=20 >&2 2>/dev/null || true
    done
    die "deployments on $ctx not ready"
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
  k "$HUB_CTX" get gridnetwork grid -o json | jq -e '[.status.routingMapStatus[]?
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
  # The chart names a peer as SWIM discovery does, <network>-<name>, except under Local membership.
  local peer=grid-$SITE
  ! local_source || peer=$SITE
  eventually "hub GridSite $peer Active (identity-verified probe)" site_phase_in "$HUB_CTX" "$peer" Active || true
  if local_source; then
    # No SWIM between the sites: the hub reaches the site only where its GridSite says.
    eventually "hub polled $SITE at its signals endpoint from its GridSite" polled "$HUB_CTX" 19090 || true
    if [[ $(k "$HUB_CTX" get gridnetwork grid -o jsonpath='{.status.connectedSites}') == 0 ]]; then
      pass "no site connected over SWIM, as the leg intends"
    else
      fail "SWIM connected the sites with no seed between them"
    fi
    for ctx in "$HUB_CTX" "$SITE_CTX"; do
      if k "$ctx" get service grid-operator-swim >/dev/null 2>&1; then
        fail "$ctx renders a SWIM Service under Local membership"
      else
        pass "$ctx runs no SWIM Service under Local membership"
      fi
    done
  else
    if [[ $SIGNED_STATE == enforce ]]; then
      eventually "site GridSite grid-hub Active over SWIM" site_phase_in "$SITE_CTX" grid-hub Active || true
    else
      # The hub gateway is ClusterIP, so the hub gossips no gateway address and stays Discovered on the site.
      eventually "site GridSite grid-hub Discovered over SWIM" site_phase_in "$SITE_CTX" grid-hub Discovered || true
    fi
    eventually "hub GridNetwork Active with >=1 connected site" network_active "$HUB_CTX" || true
  fi
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

# hub_chat <out prefix>: one chat completion through the hub gateway, succeeding on HTTP 200.
hub_chat() {
  local out=$1 url=http://127.0.0.1:18080/v1/chat/completions
  local -a tls_args=()
  if [[ $SIGNED_STATE == enforce && $HUB_ROUTER == render ]]; then
    url=https://hub.grid.internal:18080/v1/chat/completions
    tls_args=(--cacert "$WORK/id/ca.crt" --resolve hub.grid.internal:18080:127.0.0.1)
  fi
  # A gateway rollout ends the port-forward's pod, so start another.
  if [[ -z $PF_PID ]] || ! kill -0 "$PF_PID" 2>/dev/null; then
    k "$HUB_CTX" port-forward service/grid-gateway 18080:8080 >"$WORK/pf.log" 2>&1 &
    PF_PID=$!
    sleep 2
  fi
  [[ $(chat_body | curl -sS -o "$out.body" -D "$out.hdr" -w '%{http_code}' --max-time 15 \
    -H 'Content-Type: application/json' -d @- "${tls_args[@]}" "$url") == 200 ]]
}

consumer_served() { hub_chat "$WORK/consumer" && served "$WORK/consumer"; }

# local_served: a completion the hub served from its own hub-local model.
local_served() {
  hub_chat "$WORK/local" && grep -qi '^x-grid-site: hub' "$WORK/local.hdr" \
    && jq -e '.choices | length > 0' "$WORK/local.body" >/dev/null
}

# stale_hop_dropped: the rolled pod started on a serving file naming the withdrawn site's hop,
# which the operator rewrites only once the rollout is ready, and dropped it rather than exiting.
stale_hop_dropped() {
  k "$HUB_CTX" logs -l app.kubernetes.io/instance=grid-gateway -c praxis --prefix \
    --max-log-requests 10 --tail -1 2>/dev/null \
    | grep -q 'provider hops lack a matching verified TLS backend'
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
  if [[ $TRUST == pin && $code == 403 ]] && ! grep -qi '^x-grid-provider-site:' "$WORK/rogue.hdr"; then
    pass "enrolled identity the site does not pin: 403"
  elif [[ $TRUST == spiffe ]] && refused_handshake "$WORK/rogue" "$code" --cert "$d/rogue.crt" --key "$d/rogue.key"; then
    pass "enrolled identity outside spiffeIds: TLS alert, curl exit $(cat "$WORK/rogue.rc")"
  else
    fail "enrolled identity the site does not trust: HTTP $code, curl exit $(cat "$WORK/rogue.rc") ($(tr '\n' ' ' <"$WORK/rogue.err" | cut -c1-200))"
  fi
}

# ---------------------------------------------------------------------------
# Operator gridSiteRoute
# ---------------------------------------------------------------------------

# consumer_praxis: the praxis.yaml the operator generated for the hub gateway.
consumer_praxis() {
  local name
  name=$(k "$HUB_CTX" get deployment grid-gateway -o json | jq -r '.spec.template.spec.volumes[]
    | select(.configMap.name? // "" | test("consumer-config")) | .configMap.name' | head -1)
  [[ -n $name ]] && k "$HUB_CTX" get configmap "$name" -o jsonpath='{.data.praxis\.yaml}'
}

# serving_json: the serving config the operator wrote for the hub gateway.
serving_json() { k "$HUB_CTX" get configmap grid-serving-grid-grid-gateway -o jsonpath='{.data.serving-config\.json}'; }

# routes_clusters <clusters...>: the generated config routes with grid_site_route over exactly these.
routes_clusters() {
  local yaml want
  yaml=$(consumer_praxis) || return 1
  grep -q 'filter: grid_site_route' <<<"$yaml" && ! grep -q intelligent_route <<<"$yaml" || return 1
  want=$(printf '"%s", ' "$@")
  grep -qF "clusters: [${want%, }]" <<<"$yaml"
}

# hop_clusters <clusters...>: the serving config's provider hops are exactly these.
hop_clusters() {
  serving_json | jq -e --argjson want "$(jq -cn '$ARGS.positional | sort' --args "$@")" \
    '(.provider_hop_clusters // [] | sort) == $want'
}

# rolled_out: every replica runs the current template, which routes_clusters has already
# shown mounts the generated config.
rolled_out() {
  k "$HUB_CTX" get deployment grid-gateway -o json | jq -e '.metadata.generation == .status.observedGeneration
    and .status.replicas == .status.updatedReplicas and .status.updatedReplicas == .status.availableReplicas
    and (.status.unavailableReplicas // 0) == 0'
}

assert_site_route() {
  site_route || return 0
  eventually "the operator renders the hub config with grid_site_route over both derived clusters" \
    routes_clusters hub-local vcr || { consumer_praxis || true; }
  eventually "the serving config's only provider hop is the site's mTLS cluster" hop_clusters vcr \
    || { serving_json || true; }
  eventually "the hub gateway rolled out on the generated config" rolled_out || true

  # Withdraw the site's provider. The rollout runs on a praxis.yaml without it while the serving
  # file still names its hop, which must not stop the new pod.
  site_route_providers hub-local
  helm_on "$HUB_CTX" "$NS" grid-site grid-site "${HUB_SITE_ARGS[@]}" "${SR[@]}" || die "withdraw vcr on the hub"
  eventually "the withdrawn site leaves the generated config" routes_clusters hub-local || { consumer_praxis || true; }
  eventually "the withdrawn site's hop leaves the serving config" hop_clusters || { serving_json || true; }
  eventually "the gateway rolls out past the withdrawal" rolled_out || true
  eventually "the rolled pod dropped the stale hop of the withdrawn site" stale_hop_dropped || true
  eventually "the hub serves its local model with the site withdrawn" local_served || true

  site_route_providers vcr hub-local
  helm_on "$HUB_CTX" "$NS" grid-site grid-site "${HUB_SITE_ARGS[@]}" "${SR[@]}" || die "restore vcr on the hub"
  eventually "the restored site rejoins the generated config" routes_clusters hub-local vcr || true
  eventually "the restored site's hop returns" hop_clusters vcr || true
  eventually "the gateway rolls out past the restore" rolled_out || true
  eventually "the hub gateway serves the site again after the restore" consumer_served || true
}

assert_health() {
  local ctx restarts dir hits warns
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    restarts=$(kubectl --context "$ctx" get pods -A -o json | jq -r --arg a "$NS" --arg b "$ENS" '.items[]
      | select(.metadata.namespace == $a or .metadata.namespace == $b) | .metadata.name as $p
      | ((.status.initContainerStatuses // []) + (.status.containerStatuses // []))[]
      | select(.restartCount > 0)
      # Enrolling while it waits for an identity, the operator exits 0 once to restart under it.
      | select((.name == "operator" and .restartCount == 1 and .lastState.terminated.exitCode == 0) | not)
      | "\($p)/\(.name)=\(.restartCount)"')
    if [[ -z $restarts ]]; then
      pass "$ctx: no container restarts in $NS and $ENS beyond the operator's one clean restart onto its identity"
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
# High availability
# ---------------------------------------------------------------------------
#
# Faults go in while load runs, and the question is only ever what the front
# door answered. The front door is the hub gateway's LoadBalancer, not a
# port-forward: a port-forward resolves to one pod and keeps that tunnel, so it
# cannot see a replica swap at all.

# ha: this leg runs the fault scenarios.
ha() { [[ ${HA:-0} == 1 ]]; }

skip() { printf 'SKIP [%s] %s\n' "$MODE" "$*"; }

# measure <scenario> <name> <value>: a recorded value. Targets live in GRID-HA.md; nothing
# here asserts one until they do.
measure() {
  mkdir -p "$ARTIFACTS/$MODE"
  printf 'MEASURE [%s] %s %s=%s\n' "$MODE" "$1" "$2" "$3"
  printf '%s\t%s\t%s\t%s\n' "$MODE" "$1" "$2" "$3" >>"$ARTIFACTS/$MODE/ha-measures.tsv"
}

# ha_operator_args: the HA shape for the hub operator alone. renewal_args cannot carry it,
# because it runs for the site too and a hub node name there leaves the site unschedulable.
ha_operator_args() {
  OA=()
  ha || return 0
  # Keep EndpointSlice withdrawal observable to the 200ms drain watcher before the
  # operator process exits. This uses the chart's documented graceful-shutdown delay;
  # the drain assertion remains strict and still rejects a late ready endpoint.
  OA=(--set "replicaCount=$HA_OPERATOR_REPLICAS" --set shutdownDelaySeconds=3)
  # Both operator Services carry one endpoint under a Local traffic policy, and MetalLB
  # withdraws the announcement as notOwner when that endpoint changes node. Pinning the
  # operator keeps the lab's load balancer out of a test about the grid.
  [[ -z ${HA_OPERATOR_NODE:-} ]] || OA+=(--set "nodeSelector.kubernetes\.io/hostname=$HA_OPERATOR_NODE")
}

# ha_gateway_args: the HA shape for the hub gateway. The spread is strict on purpose: with
# both replicas on one node the drain scenario proves nothing.
ha_gateway_args() {
  GA=()
  ha || return 0
  GA=(--set "replicaCount=$HA_GATEWAY_REPLICAS"
    --set "topologySpreadConstraints[0].maxSkew=1"
    --set "topologySpreadConstraints[0].topologyKey=kubernetes.io/hostname"
    --set "topologySpreadConstraints[0].whenUnsatisfiable=DoNotSchedule"
    # Without this the tainted control-plane counts as a third domain it can never fill, so
    # the surge pod of a rolling update satisfies maxSkew nowhere and the rollout stalls.
    --set "topologySpreadConstraints[0].nodeTaintsPolicy=Honor"
    --set "topologySpreadConstraints[0].labelSelector.matchLabels.app\.kubernetes\.io/instance=grid-gateway")
}

# front_door_lb: a LoadBalancer beside the gateway's own Service, so load reaches the pods
# through kube-proxy rather than a port-forward. Signed-state gateways already have the
# front door address on their Service, so reuse that Service instead of sharing its IP.
front_door_lb() {
  [[ $SIGNED_STATE != enforce ]] || return 0
  k "$HUB_CTX" get service grid-gateway -o json \
    | jq --arg ip "$HUB_GW_IP" '{apiVersion: "v1", kind: "Service",
        metadata: {name: "grid-gateway-frontdoor", namespace: .metadata.namespace},
        spec: {type: "LoadBalancer", loadBalancerIP: $ip, selector: .spec.selector,
          ports: [.spec.ports[] | {name, port, targetPort, protocol}]}}' \
    | k "$HUB_CTX" apply -f - >/dev/null
}

front_door_lb_ready() {
  local service=grid-gateway-frontdoor
  [[ $SIGNED_STATE != enforce ]] || service=grid-gateway
  [[ $(k "$HUB_CTX" get service "$service" \
    -o jsonpath='{.status.loadBalancer.ingress[0].ip}') == "$HUB_GW_IP" ]]
}

# front_door <out prefix>: one chat completion through the hub gateway Service. Prints
# "<code> <provider site>", 000 for a call that never got a response.
front_door() {
  local out=$1 code site url="http://$HUB_GW_IP:8080/v1/chat/completions"
  local -a tls_args=()
  if [[ $SIGNED_STATE == enforce && $HUB_ROUTER == render ]]; then
    url=https://hub.grid.internal:8080/v1/chat/completions
    tls_args=(--cacert "$WORK/id/ca.crt" --resolve "hub.grid.internal:8080:$HUB_GW_IP")
  fi
  code=$(chat_body | lb_curl -sS -o "$out.body" -D "$out.hdr" -w '%{http_code}' --max-time 10 \
    -H 'Content-Type: application/json' -d @- "${tls_args[@]}" "$url" 2>/dev/null) || true
  site=$(grep -i '^x-grid-provider-site:' "$out.hdr" 2>/dev/null | head -1 | cut -d: -f2- | tr -d ' \r')
  printf '%s %s\n' "${code:-000}" "${site:--}"
}

front_door_ok() { [[ $(front_door "$WORK/fd-probe") == "200 $SITE" ]]; }

# watch_front_door: one call a second, recorded whatever it answers. No retry: a retried call
# averages away the outage this leg exists to measure.
watch_front_door() {
  trap 'exit 0' TERM
  while :; do
    printf '%s %s\n' "$(date -u +%s)" "$(front_door "$WORK/frontdoor")" >>"$WORK/frontdoor.log"
    sleep 1
  done
}

start_front_door() {
  : >"$WORK/frontdoor.log"
  front_door_lb || { fail "create the front door LoadBalancer"; return 1; }
  eventually "the front door LoadBalancer has address $HUB_GW_IP" front_door_lb_ready || return 1
  eventually "the hub gateway answers on its LoadBalancer $HUB_GW_IP" front_door_ok || return 1
  watch_front_door &
  FRONTDOOR_PID=$!
}

stop_front_door() {
  [[ -z $FRONTDOOR_PID ]] || kill "$FRONTDOOR_PID" 2>/dev/null || true
  FRONTDOOR_PID=""
}

# front_door_result <label> <from epoch> <strict|lenient>: no 5xx and nothing served from
# outside the site. strict also means no call failed to connect; lenient records those
# instead, because MetalLB re-announces a drained node's address and kind cannot avoid it.
front_door_result() {
  local label=$1 from=$2 how=$3 calls bad connect
  calls=$(awk -v f="$from" '$1 + 0 >= f' "$WORK/frontdoor.log" | wc -l)
  bad=$(awk -v f="$from" -v s="$SITE" '$1 + 0 >= f && (($2 >= 500 && $2 < 600) || ($2 == "200" && $3 != s))' \
    "$WORK/frontdoor.log")
  connect=$(awk -v f="$from" '$1 + 0 >= f && $2 == "000"' "$WORK/frontdoor.log" | wc -l)
  if ((calls == 0)); then
    fail "$label: the front door monitor recorded no calls"
  elif [[ -n $bad ]]; then
    fail "$label: $(grep -c . <<<"$bad") of $calls front door calls were 5xx or served from elsewhere"
    head -5 <<<"$bad"
  else
    pass "$label: $calls front door calls, no 5xx and none served from outside $SITE"
  fi
  if [[ $how == strict ]]; then
    if ((connect == 0)); then
      pass "$label: every front door call connected"
    else
      fail "$label: $connect of $calls front door calls never connected"
    fi
  else
    measure "$label" connect_failures "$connect"
  fi
}

# measure_ttr <label> <name> <from epoch> <code|draw|catalog>: seconds from the fault to the
# first good read after the last bad one, 0 when the fault produced none. code and draw read
# the front door log, catalog the observer log, where good means providers came back.
measure_ttr() {
  local log=$WORK/frontdoor.log v
  [[ $4 != catalog ]] || log=$WORK/catalog.log
  v=$(awk -v f="$3" -v s="$SITE" -v want="$4" '
    $1 + 0 < f { next }
    { if (want == "catalog") bad = ($2 != "200") || ($3 + 0 == 0)
      else bad = ($2 != "200") || (want == "draw" && $3 != s)
      if (bad) { last = $1; rec = "" } else if (last != "" && rec == "") { rec = $1 } }
    END { if (last == "") print 0; else if (rec == "") print "unrecovered"; else print rec - f }
  ' "$log")
  measure "$1" "$2" "$v"
}

# watch_drain <context> <pod> <service>: watch EndpointSlice changes and whether the pod's
# containers are still running. Watching changes catches a short handoff between holders.
watch_drain() {
  local ctx=$1 pod=$2 svc=$3 ip=$4 selector endpoint_json resource_version target_slice event_pid event_fd event event_type event_slice ready running
  trap 'exit 0' TERM
  selector="kubernetes.io/service-name=$svc"
  endpoint_json=$(k "$ctx" get endpointslices -l "$selector" -o json 2>/dev/null) || endpoint_json='{"items":[]}'
  resource_version=$(jq -r '.metadata.resourceVersion // empty' <<<"$endpoint_json")
  target_slice=$(endpoint_slice_for_ip "$ip" "$endpoint_json")
  ready=$(endpoint_status "$ip" "$endpoint_json")
  running=$(pod_running_status "$ctx" "$pod")
  printf '%s %s %s\n' "$(date -u +%s)" "$ready" "$running" >>"$WORK/drain.log"

  if [[ -n $resource_version ]]; then
    coproc DRAIN_EVENTS {
      k "$ctx" get endpointslices -l "$selector" --watch-only --resource-version="$resource_version" \
        --output-watch-events -o json 2>/dev/null | jq --unbuffered -c .
    }
  else
    coproc DRAIN_EVENTS {
      k "$ctx" get endpointslices -l "$selector" --watch-only --output-watch-events -o json 2>/dev/null \
        | jq --unbuffered -c .
    }
  fi
  event_pid=$DRAIN_EVENTS_PID
  event_fd=${DRAIN_EVENTS[0]}
  trap 'kill "$event_pid" 2>/dev/null || true; exit 0' TERM
  while :; do
    if IFS= read -r -t 0.2 -u "$event_fd" event; then
      event_type=$(jq -r '.type // empty' <<<"$event")
      event_slice=$(jq -r '.object.metadata.name // empty' <<<"$event")
      if jq -e --arg ip "$ip" '(.object.endpoints[]? | select((.addresses // []) | index($ip))) != null' \
        <<<"$event" >/dev/null; then
        target_slice=$event_slice
        ready=$(endpoint_event_status "$ip" "$event")
      elif [[ -n $target_slice && $event_slice == "$target_slice" ]]; then
        ready=$(endpoint_event_status "$ip" "$event")
      else
        continue
      fi
      [[ $event_type != DELETED ]] || target_slice=""
      running=$(pod_running_status "$ctx" "$pod")
      printf '%s %s %s\n' "$(date -u +%s)" "$ready" "$running" >>"$WORK/drain.log"
    else
      running=$(pod_running_status "$ctx" "$pod")
      if [[ $running != true ]]; then
        endpoint_json=$(k "$ctx" get endpointslices -l "$selector" -o json 2>/dev/null) || endpoint_json='{"items":[]}'
        ready=$(endpoint_status "$ip" "$endpoint_json")
        printf '%s %s %s\n' "$(date -u +%s)" "$ready" "$running" >>"$WORK/drain.log"
        break
      fi
      if ! kill -0 "$event_pid" 2>/dev/null; then
        endpoint_json=$(k "$ctx" get endpointslices -l "$selector" -o json 2>/dev/null) || endpoint_json='{"items":[]}'
        ready=$(endpoint_status "$ip" "$endpoint_json")
        printf '%s %s %s\n' "$(date -u +%s)" "$ready" "$running" >>"$WORK/drain.log"
      fi
    fi
  done
}

endpoint_slice_for_ip() {
  jq -r --arg ip "$1" '[.items[] | select(any(.endpoints[]?; (.addresses // []) | index($ip))) | .metadata.name][0] // empty' \
    <<<"$2"
}

endpoint_status() {
  jq -r --arg ip "$1" '[.items[].endpoints[]? | select((.addresses // []) | index($ip))
      | (if (.conditions.terminating // false) then false else (if .conditions.ready == null then true else .conditions.ready end) end)]
      | if length == 0 then "gone" elif any then "true" else "false" end' <<<"$2"
}

endpoint_event_status() {
  jq -r --arg ip "$1" '
    if .type == "DELETED" then "gone"
    else [(.object.endpoints // [])[] | select((.addresses // []) | index($ip))
      | (if (.conditions.terminating // false) then false else (if .conditions.ready == null then true else .conditions.ready end) end)]
      | if length == 0 then "gone" elif any then "true" else "false" end
    end' <<<"$2"
}

pod_running_status() {
  local running
  if running=$(k "$1" get pod "$2" -o jsonpath='{.status.containerStatuses[*].state.running.startedAt}' 2>/dev/null); then
    [[ -n $running ]] && printf 'true' || printf 'false'
  else
    printf 'gone'
  fi
}

# start_drain_watch <context> <pod> <service>
start_drain_watch() {
  local ip
  ip=$(k "$1" get pod "$2" -o jsonpath='{.status.podIP}') || { fail "no address for pod $2"; return 1; }
  : >"$WORK/drain.log"
  watch_drain "$1" "$2" "$3" "$ip" &
  DRAIN_PID=$!
}

stop_drain_watch() {
  if [[ -n $DRAIN_PID ]]; then
    kill "$DRAIN_PID" 2>/dev/null || true
    wait "$DRAIN_PID" 2>/dev/null || true
  fi
  DRAIN_PID=""
}

# keep <name>: a monitor log into the artifacts, since WORK is a mktemp dir. A measured claim
# with no retained samples cannot be checked after the fact.
keep() {
  mkdir -p "$ARTIFACTS/$MODE"
  [[ ! -s "$WORK/$1.log" ]] || cp "$WORK/$1.log" "$ARTIFACTS/$MODE/$1-$2.log"
}

# drain_order_samples: samples where the pod had left the ready endpoints but was still running.
drain_order_samples() { awk '($2 == "false" || $2 == "gone") && $3 == "true"' "$WORK/drain.log" | grep -c . || true; }

# late_endpoint_samples: samples where the pod was still a ready endpoint after it stopped.
late_endpoint_samples() { awk '$2 == "true" && ($3 == "false" || $3 == "gone")' "$WORK/drain.log" | grep -c . || true; }

# assert_drain_order <label>: readiness left the Service while the container still ran, and the
# pod was never a ready endpoint after it stopped.
assert_drain_order() {
  local early late
  early=$(drain_order_samples)
  late=$(late_endpoint_samples)
  # A late ready endpoint is the defect; an unobserved window is only an unobserved window.
  if ((late > 0)); then
    fail "$1: the pod was a ready endpoint after its container stopped ($late samples)"
  elif ((early == 0)); then
    fail "$1: never saw the pod leave the Service's ready endpoints with its container still running"
  else
    pass "$1: the pod left the Service's ready endpoints while it still ran ($early samples)"
  fi
}

# pod_absent <pod>: the pod object is gone, which is the end of its drain.
pod_absent() {
  local pod
  pod=$(k "$HUB_CTX" get pod "$1" --ignore-not-found -o name) || return 1
  [[ -z $pod ]]
}

# selftest_pod_absent: NotFound is absence, while an API error is not.
selftest_pod_absent() (
  HUB_CTX=selftest
  k() {
    [[ $2 == get && $3 == pod && $5 == --ignore-not-found && $6 == -o && $7 == name ]] || return 2
    case $4 in
      missing) return 0 ;;
      present) printf 'pod/present\n' ;;
      api-error) return 1 ;;
      *) return 2 ;;
    esac
  }
  pod_absent missing && ! pod_absent present && ! pod_absent api-error
)

# gateway_pod: the oldest gateway pod, so a scenario hits one that was already serving.
gateway_pod() {
  k "$HUB_CTX" get pods -l app.kubernetes.io/instance=grid-gateway \
    --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[0].metadata.name}'
}

# operator_prestop: the operator pod holds its endpoint open after the probe fails.
operator_prestop() {
  k "$HUB_CTX" get deployment grid-operator \
    -o jsonpath='{.spec.template.spec.containers[*].lifecycle.preStop.exec.command[*]}' 2>/dev/null \
    | grep -q sleep
}

# operator_pod: the lease holder, whose drain is worth watching. A candidate's address is in no
# holder slice, so watching one would pass the drain-order check over nothing.
operator_pod() {
  local holder
  holder=$(lease_holder)
  if [[ -n $holder ]]; then
    printf '%s' "$holder"
  else
    k "$HUB_CTX" get pods -l app.kubernetes.io/instance=grid-operator \
      -o jsonpath='{.items[0].metadata.name}'
  fi
}

# ready_replicas <deployment>
ready_replicas() {
  local n
  n=$(k "$HUB_CTX" get deployment "$1" -o jsonpath='{.status.readyReplicas}' 2>/dev/null)
  printf '%s' "${n:-0}"
}

gateway_ready_at_least() { (($(ready_replicas grid-gateway) >= $1)); }

# catalog_served: the hub's signals rollup, which an observer reads through the Service.
# Prints "<code> <provider series>".
catalog_served() {
  local out="$WORK/catalog" code n
  code=$(lb_curl -sS -o "$out.body" -w '%{http_code}' --max-time 5 \
    --resolve "hub.grid.internal:9091:$HUB_SIG_IP" --cacert "$WORK/id/ca.crt" \
    --cert "$WORK/id/hub.crt" --key "$WORK/id/hub.key" \
    "https://hub.grid.internal:9091/v1/site/signals" 2>/dev/null) || true
  n=$(grep -c 'grid_provider="' "$out.body" 2>/dev/null) || n=0
  printf '%s %s\n' "${code:-000}" "$n"
}

# watch_catalog: twice a second, what an observer reading the signals Service gets. A pod that
# is a ready endpoint before it has published anything answers 200 with no provider series,
# which drops this site from the reader's draw.
watch_catalog() {
  trap 'exit 0' TERM
  while :; do
    printf '%s %s\n' "$(date -u +%s)" "$(catalog_served)" >>"$WORK/catalog.log"
    sleep 0.5
  done
}

start_catalog_watch() {
  : >"$WORK/catalog.log"
  watch_catalog &
  CATALOG_PID=$!
}

stop_catalog_watch() {
  [[ -z $CATALOG_PID ]] || kill "$CATALOG_PID" 2>/dev/null || true
  CATALOG_PID=""
}

# assert_catalog <label> <from epoch>: no observer read a 200 carrying zero providers. On main
# signals_handler always answers 200 from whatever is published, so a pod that has not published
# yet serves an empty catalog; 6a53a02c answers 503 until the first publish and is not on main.
assert_catalog() {
  local reads empty unreachable
  reads=$(awk -v f="$2" '$1 + 0 >= f' "$WORK/catalog.log" | grep -c . || true)
  empty=$(awk -v f="$2" '$1 + 0 >= f && $2 == "200" && $3 + 0 == 0' "$WORK/catalog.log" | grep -c . || true)
  unreachable=$(awk -v f="$2" '$1 + 0 >= f && $2 == "000"' "$WORK/catalog.log" | grep -c . || true)
  # The signals Service is externalTrafficPolicy Local, so while no operator pod is a ready
  # endpoint MetalLB withdraws the address and the read cannot connect. Recorded, because a
  # pass over nothing but unreachable reads has exercised nothing.
  measure "$1" catalog_reads "$reads"
  measure "$1" catalog_unreachable_reads "$unreachable"
  if ((reads == 0)); then
    fail "$1: the catalog monitor recorded no reads"
  elif ((empty == 0)); then
    pass "$1: $reads observer reads, none a 200 with an empty provider catalog"
  else
    fail "$1: $empty of $reads observer reads were 200 with no providers"
  fi
}

# ha_scenario <name> <function>: inject, settle, then account for the front door.
ha_scenario() {
  local name=$1 fn=$2 strict=${3:-strict} from
  from=$(date -u +%s)
  log "[$MODE] HA scenario: $name"
  "$fn" || fail "$name: injecting the fault failed"
  eventually "$name: the gateway is rolled out again" rolled_out || true
  eventually "$name: the front door serves $SITE again" front_door_ok || true
  front_door_result "$name" "$from" "$strict"
  measure_ttr "$name" ttr_front_door_seconds "$from" code
  measure_ttr "$name" ttr_in_draw_seconds "$from" draw
}

# Scenario 1: a rolling restart of the gateway.
ha_gateway_rollout() {
  local pod
  pod=$(gateway_pod) || return 1
  start_drain_watch "$HUB_CTX" "$pod" grid-gateway
  k "$HUB_CTX" rollout restart deployment/grid-gateway >/dev/null || return 1
  k "$HUB_CTX" rollout status deployment/grid-gateway --timeout="${TIMEOUT}s" >/dev/null || true
  # Watch past the rollout: a window that opens as the last old pod goes would otherwise fall
  # outside the watch, and the assertion would report an absence it never looked for.
  promptly "gateway rolling restart: the restarted pod is gone" pod_absent "$pod" || true
  stop_drain_watch
  keep drain gateway-rolling-restart
  assert_drain_order "gateway rolling restart"
}

# Scenario 2: one gateway pod deleted.
ha_gateway_pod_deleted() {
  local pod
  pod=$(gateway_pod) || return 1
  start_drain_watch "$HUB_CTX" "$pod" grid-gateway
  k "$HUB_CTX" delete pod "$pod" --wait=false >/dev/null || return 1
  promptly "gateway pod deleted: the deleted pod is gone" pod_absent "$pod" || true
  eventually "gateway pod deleted: $HA_GATEWAY_REPLICAS replicas ready again" \
    gateway_ready_at_least "$HA_GATEWAY_REPLICAS" || true
  stop_drain_watch
  keep drain gateway-pod-deleted
  assert_drain_order "gateway pod deleted"
}

# Scenario 3: the operator restarts. The chart rolls at every replica count: the surge pod
# waits as a candidate and takes the lease when the holder releases it.
ha_operator_restart() {
  local pod from
  pod=$(operator_pod) || return 1
  from=$(date -u +%s)
  start_drain_watch "$HUB_CTX" "$pod" grid-operator-signals
  k "$HUB_CTX" rollout restart deployment/grid-operator >/dev/null || return 1
  local rolled=0 started
  started=$(date -u +%s)
  k "$HUB_CTX" rollout status deployment/grid-operator --timeout="${TIMEOUT}s" >/dev/null 2>&1 && rolled=1
  measure "operator restart" ttr_rollout_seconds "$(($(date -u +%s) - started))"
  # Deployment rollout completion does not wait for terminating old Pods to disappear.
  # Keep the drain watcher running through that final termination window.
  promptly "operator restart: the restarted pod is gone" pod_absent "$pod" || true
  stop_drain_watch
  keep drain operator-restart
  measure "operator restart" drain_order_samples "$(drain_order_samples)"
  measure "operator restart" late_endpoint_samples "$(late_endpoint_samples)"
  # The holder withdraws its own slices, so the order is the operator's to get right. At one
  # replica without a preStop delay the window can be shorter than a sample.
  if ((HA_OPERATOR_REPLICAS > 1)) || operator_prestop; then
    assert_drain_order "operator restart"
  else
    skip "operator restart: one replica and no preStop delay, so the drain order is recorded only"
  fi
  ! signals_running || assert_catalog "operator restart" "$from"
  if ((HA_OPERATOR_REPLICAS > 1)); then
    # A rollout that never finishes is what an unready candidate does at maxUnavailable 0.
    if ((rolled)); then
      pass "operator restart: the rollout completed"
    else
      fail "operator restart: the rollout did not complete within ${TIMEOUT}s"
    fi
    assert_operator_available
    if signals_running; then
      promptly "operator restart: the signals Service returned to the holder" signals_targets_holder || true
      measure "operator restart" ttr_signals_endpoint_seconds "$(($(date -u +%s) - from))"
      assert_signals_targets_holder "operator restart"
    fi
  else
    skip "operator restart: the handover checks need two replicas"
  fi
}

# Scenario 5: cordon and drain the node running a gateway replica.
ha_node_drain() {
  local pod node
  # Not the operator's node: draining that moves its single Local-policy endpoint, which is
  # the lab limitation this leg pins the operator to avoid, not something to test here.
  pod=$(k "$HUB_CTX" get pods -l app.kubernetes.io/instance=grid-gateway -o json \
    | jq -r --arg skip "${HA_OPERATOR_NODE:-}" '[.items[] | select(.spec.nodeName != $skip)][0].metadata.name // ""')
  [[ -n $pod ]] || { skip "node drain: every gateway replica shares the operator's node"; return 0; }
  node=$(k "$HUB_CTX" get pod "$pod" -o jsonpath='{.spec.nodeName}') || return 1
  if [[ $(kubectl --context "$HUB_CTX" get nodes -o json | jq '[.items[] | select(.spec.taints // [] | any(.key == "node-role.kubernetes.io/control-plane") | not)] | length') -lt 2 ]]; then
    skip "node drain: the hub has one schedulable node, so draining it takes the whole site"
    return 0
  fi
  log "[$MODE] draining $node, which runs $pod"
  # Uncordon on every path, so a failed drain or an early return cannot leave the node cordoned.
  CORDONED=$node
  kubectl --context "$HUB_CTX" drain "$node" --ignore-daemonsets --delete-emptydir-data \
    --timeout=120s >/dev/null || return 1
  eventually "node drain: a ready gateway replica remains" gateway_ready_at_least 1 || true
  # The strict spread refuses to put the evicted replica on the surviving node, so the
  # Deployment runs short until the node returns. The gateway chart has no PodDisruptionBudget;
  # the operator chart renders one above one replica.
  measure "node drain" ready_replicas_while_drained "$(ready_replicas grid-gateway)"
  uncordon_node
  eventually "node drain: the gateway returns to $HA_GATEWAY_REPLICAS ready replicas" \
    gateway_ready_at_least "$HA_GATEWAY_REPLICAS" || true
}

# uncordon_node: uncordons whatever this leg cordoned, from the scenario or from cleanup.
uncordon_node() {
  [[ -n ${CORDONED:-} ]] || return 0
  kubectl --context "$HUB_CTX" uncordon "$CORDONED" >/dev/null 2>&1 || true
  CORDONED=""
}

# assert_ha: runs after assert_serving, which wrote the identity files the catalog read uses.
assert_ha() {
  ha || return 0
  eventually "the hub gateway runs $HA_GATEWAY_REPLICAS ready replicas" \
    gateway_ready_at_least "$HA_GATEWAY_REPLICAS" || return 0
  start_front_door || { fail "the front door monitor never got a first good call"; return 0; }
  ! signals_running || start_catalog_watch
  ha_scenario "gateway rolling restart" ha_gateway_rollout
  ha_scenario "gateway pod deleted" ha_gateway_pod_deleted
  ha_scenario "operator restart" ha_operator_restart
  ha_scenario "node drain" ha_node_drain lenient
  assert_leader_election
  stop_catalog_watch
  stop_front_door
  keep frontdoor all
  keep catalog all
}

# ---------------------------------------------------------------------------
# Operator leader election
# ---------------------------------------------------------------------------
#
# A second operator replica only means anything once the chart accepts one, so
# every assertion here is driven by what the chart renders, not by a branch name.

# operator_ha: the chart accepts more than one operator replica.
operator_ha() { helm template grid-operator "$ROOT/charts/grid-operator" --set replicaCount=2 >/dev/null 2>&1; }

lease_field() { k "$HUB_CTX" get lease grid-operator -o "jsonpath={$1}" 2>/dev/null; }
lease_holder() { lease_field .spec.holderIdentity; }
lease_transitions() { printf '%s' "$(lease_field .spec.leaseTransitions)"; }

# ready_operator_pods: operator pods whose Ready condition is true.
ready_operator_pods() {
  k "$HUB_CTX" get pods -l app.kubernetes.io/instance=grid-operator -o json \
    | jq '[.items[] | select((.status.conditions // [])[] | select(.type == "Ready") | .status == "True")] | length'
}

# holder_changed <from>: the lease named someone else.
holder_changed() {
  local now
  now=$(lease_holder)
  [[ -n $now && $now != "$1" ]]
}

# assert_operator_available: every replica counts toward the Deployment's availability. A
# candidate that stays unready pins availableReplicas below replicas, which stalls a
# RollingUpdate at the default maxUnavailable of 0 and leaves Argo CD reporting Progressing for good.
assert_operator_available() {
  local want have
  want=$(k "$HUB_CTX" get deployment grid-operator -o jsonpath='{.spec.replicas}')
  have=$(k "$HUB_CTX" get deployment grid-operator -o jsonpath='{.status.availableReplicas}')
  if [[ ${have:-0} == "$want" ]]; then
    pass "operator availability: $have of $want replicas available"
  else
    fail "operator availability: ${have:-0} of $want replicas available, so a RollingUpdate cannot proceed at the default maxUnavailable of 0"
  fi
}

# assert_operator_pdb: the budget leaves room for one eviction. A budget of minAvailable 1 over
# a single ready pod allows none, and a node drain then blocks until it times out.
assert_operator_pdb() {
  local allowed
  if ! k "$HUB_CTX" get pdb grid-operator >/dev/null 2>&1; then
    fail "operator PodDisruptionBudget: none rendered at $HA_OPERATOR_REPLICAS replicas"
    return
  fi
  allowed=$(k "$HUB_CTX" get pdb grid-operator -o jsonpath='{.status.disruptionsAllowed}')
  if ((${allowed:-0} >= 1)); then
    pass "operator PodDisruptionBudget: $allowed disruption allowed"
  else
    fail "operator PodDisruptionBudget: ${allowed:-0} disruptions allowed, so every eviction is refused and a node drain blocks"
  fi
}

# Peer traffic reaches one operator, and which mechanism puts it there is not this leg's
# business: a label the Service selects and an EndpointSlice the operator writes have to hold
# the same invariant, that the signals Service's ready addresses are the holder's and nobody
# else's. Asserting the addresses rather than the mechanism also catches the conflict case,
# where a Service keeps its selector and the generated slice adds the candidates back beside
# the operator's own.

# signals_endpoint_ips: every ready address across every EndpointSlice of the signals Service.
signals_endpoint_ips() {
  k "$HUB_CTX" get endpointslices -l 'kubernetes.io/service-name=grid-operator-signals' -o json 2>/dev/null \
    | jq -r '[.items[].endpoints[]? | select((.conditions.ready // true)) | .addresses[]?] | unique | .[]'
}

# holder_ip: the address of the pod the lease names.
holder_ip() {
  local holder
  holder=$(lease_holder) || return 1
  [[ -n $holder ]] || return 1
  k "$HUB_CTX" get pod "$holder" -o jsonpath='{.status.podIP}' 2>/dev/null
}

# signals_targets_holder: the ready address set is exactly the holder's address.
signals_targets_holder() {
  local want got
  want=$(holder_ip) || return 1
  got=$(signals_endpoint_ips)
  [[ -n $want && $got == "$want" ]]
}

# assert_signals_targets_holder <label>
assert_signals_targets_holder() {
  local want got
  want=$(holder_ip)
  got=$(signals_endpoint_ips | tr '\n' ' ')
  if [[ -n $want && ${got% } == "$want" ]]; then
    pass "$1: the signals Service reaches the holder alone at $want"
  else
    fail "$1: the signals Service reaches [${got% }], and the holder is at ${want:-none}"
  fi
}

# signals_running: the transport the grid resolved. Under gossip the operator runs no signals
# listener, scraper or poller, since all three key on the mode (main.rs:237), so the signals
# Service fronts nothing and a handover number taken there measures an empty address.
signals_running() {
  [[ $(k "$HUB_CTX" get gridnetwork grid -o jsonpath='{.spec.signalTransport.mode}' 2>/dev/null) == poll ]]
}

# signals_local_policy: the signals Service routes only to endpoints on the receiving node.
signals_local_policy() {
  [[ $(k "$HUB_CTX" get service grid-operator-signals -o jsonpath='{.spec.externalTrafficPolicy}' 2>/dev/null) == Local ]]
}

# assert_signals_endpoint_node <label>: under a Local traffic policy kube-proxy decides
# locality from the slice's nodeName, so an endpoint written without one is local to no node,
# the load balancer finds every node unhealthy, and the address black holes.
assert_signals_endpoint_node() {
  local want got
  signals_local_policy || { skip "$1: the signals Service does not use a Local traffic policy"; return 0; }
  want=$(k "$HUB_CTX" get pod "$(lease_holder)" -o jsonpath='{.spec.nodeName}' 2>/dev/null)
  got=$(k "$HUB_CTX" get endpointslices -l 'kubernetes.io/service-name=grid-operator-signals' -o json 2>/dev/null \
    | jq -r --arg ip "$(holder_ip)" '[.items[].endpoints[]?
        | select((.addresses // []) | index($ip)) | .nodeName // "none"] | unique | join(",")')
  if [[ -n $want && $got == "$want" ]]; then
    pass "$1: the holder's endpoint names its node $want"
  else
    fail "$1: the holder's endpoint names node [$got], and the holder runs on ${want:-none}"
  fi
}

# log_epoch <file> <text>: the first matching line's timestamp, in epoch seconds with
# milliseconds, so lines from two pods can be subtracted.
log_epoch() {
  local t
  t=$(sed 's/\x1b\[[0-9;]*m//g' "$1" 2>/dev/null | grep -m1 -F "$2" \
    | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]+Z' | head -1)
  [[ -n $t ]] || return 1
  date -d "$t" +%s.%3N
}

# ms_between <from> <to>
ms_between() { awk -v a="$1" -v b="$2" 'BEGIN { printf "%d", (b - a) * 1000 }'; }

# measure_handover <label>: the operator's own handover, taken from its logs. A wall clock
# wrapped around kubectl delete spans the whole termination and cannot see a sub-second
# mechanism, so it stays beside this under its own name rather than standing in for it.
measure_handover() {
  local slug=${1// /-} old="$ARTIFACTS/$MODE/holder-old-${1// /-}.log" acq rel term
  acq=$(log_epoch "$ARTIFACTS/$MODE/holder-$slug.log" "holding the operator lease") || return 0
  if rel=$(log_epoch "$old" "released the operator lease"); then
    measure "$1" release_to_hold_ms "$(ms_between "$rel" "$acq")"
    ! term=$(log_epoch "$old" "standing down") \
      || measure "$1" standdown_sigterm_to_release_ms "$(ms_between "$term" "$rel")"
  else
    # No release line is the point of the kill: the successor waits the lease out instead.
    measure "$1" release_to_hold_ms none
  fi
}

# keep_holder_log <label>: the new holder's own log. A later scenario's delete takes the pod
# with it, and the lease and routing lines live nowhere else.
keep_holder_log() {
  local holder slug=${1// /-}
  holder=$(lease_holder) || return 0
  [[ -n $holder ]] || return 0
  mkdir -p "$ARTIFACTS/$MODE"
  k "$HUB_CTX" logs "$holder" --all-containers --prefix >"$ARTIFACTS/$MODE/holder-$slug.log" 2>&1 || true
}

# signals_from_holder: the holder pod's own listener over a port-forward, so this timing
# excludes the Service and the load balancer. Prints "<code> <provider series>".
signals_from_holder() {
  local holder pid out="$WORK/holder-signals" code n
  holder=$(lease_holder) || return 1
  [[ -n $holder ]] || return 1
  k "$HUB_CTX" port-forward "pod/$holder" 19191:9091 >/dev/null 2>&1 &
  pid=$!
  sleep 1
  code=$(curl -sS -o "$out.body" -w '%{http_code}' --max-time 5 \
    --resolve "hub.grid.internal:19191:127.0.0.1" --cacert "$WORK/id/ca.crt" \
    --cert "$WORK/id/hub.crt" --key "$WORK/id/hub.key" \
    "https://hub.grid.internal:19191/v1/site/signals" 2>/dev/null) || true
  kill "$pid" 2>/dev/null || true
  n=$(grep -c 'grid_provider="' "$out.body" 2>/dev/null) || n=0
  printf '%s %s\n' "${code:-000}" "$n"
}

holder_signals_ok() {
  local r
  r=$(signals_from_holder) || return 1
  [[ ${r%% *} == 200 && ${r##* } -gt 0 ]]
}

# sigkill_holder: SIGKILL the holder's process inside its node, so it cannot release the
# lease. Deleting the pod, even with --force --grace-period=0, lets the operator release on
# the way out, which measures the graceful path under another name.
sigkill_holder() {
  local node
  node=$(k "$HUB_CTX" get pod "$1" -o jsonpath='{.spec.nodeName}') || return 1
  # pkill would need a name, and the process is called operator with a candidate beside it on
  # the same node under the pin. crictl stop with no timeout kills this pod's container alone.
  "$CONTAINER_TOOL" exec "$node" bash -c "
    set -e
    sandbox=\$(crictl pods -q --name '$1')
    container=\$(crictl ps -q --pod \"\$sandbox\" | head -1)
    [ -n \"\$container\" ]
    crictl stop --timeout 0 \"\$container\" >/dev/null"
  # The process is dead and cannot release, and removing the object takes the identity with
  # it. Killing alone would let kubelet restart it in place under the same holder name.
  k "$HUB_CTX" delete pod "$1" --grace-period=0 --force >/dev/null 2>&1 || true
}

# ha_leader_kill <label> <sigkill|delete args...>: take the holder out, watch the lease move.
ha_leader_kill() {
  local label=$1 holder before from
  shift
  holder=$(lease_holder)
  before=$(lease_transitions)
  [[ -n $holder ]] || { fail "$label: no lease holder to remove"; return 1; }
  from=$(date -u +%s)
  log "[$MODE] $label: removing the holder $holder"
  # The stand-down lines are written while the pod terminates, so follow its log from before
  # the fault until it goes. Read afterwards, the pod is already gone and they are lost.
  local follow
  mkdir -p "$ARTIFACTS/$MODE"
  k "$HUB_CTX" logs -f "$holder" --all-containers --prefix \
    >"$ARTIFACTS/$MODE/holder-old-${label// /-}.log" 2>&1 &
  follow=$!
  if [[ ${1:-} == sigkill ]]; then
    sigkill_holder "$holder" || { fail "$label: could not signal the holder"; return 1; }
  else
    k "$HUB_CTX" delete pod "$holder" "$@" >/dev/null || return 1
  fi
  promptly "$label: the lease named a new holder" holder_changed "$holder" || true
  measure "$label" ttr_lease_handover_seconds "$(($(date -u +%s) - from))"
  ! signals_running || measure_ttr "$label" ttr_leader_write_seconds "$from" catalog
  local after
  after=$(lease_transitions)
  if ((${after:-0} > ${before:-0})); then
    pass "$label: leaseTransitions went from ${before:-0} to ${after:-0}"
  else
    fail "$label: leaseTransitions stayed at ${before:-0}"
  fi
  if signals_running; then
    promptly "$label: the new holder's own signals listener answers with providers" \
      holder_signals_ok || true
    measure "$label" ttr_signals_listener_seconds "$(($(date -u +%s) - from))"
    promptly "$label: the signals Service followed the lease to the new holder" \
      signals_targets_holder || true
    measure "$label" ttr_signals_endpoint_seconds "$(($(date -u +%s) - from))"
    assert_signals_targets_holder "$label"
    assert_signals_endpoint_node "$label"
    assert_catalog "$label" "$from"
  else
    skip "$label: no signals listener runs under this transport, so there is nothing to time"
  fi
  kill "$follow" 2>/dev/null || true
  keep_holder_log "$label"
  measure_handover "$label"
}



assert_leader_election() {
  if ! operator_ha; then
    skip "operator leader election: the chart still pins replicaCount to 1, so there is no lease to move"
    measure "operator leader kill" ttr_leader_write_seconds skipped
    return 0
  fi
  eventually "the operator lease has a holder" lease_holder || { fail "no operator lease"; return 0; }
  assert_operator_available
  assert_operator_pdb
  if signals_running; then
    assert_signals_targets_holder "operator steady state"
    assert_signals_endpoint_node "operator steady state"
  else
    skip "operator steady state: no signals listener runs under this transport"
  fi
  # One kill leaves the lease to expire and the other releases it, so both are recorded: the
  # gap between them is what releasing on stand-down buys.
  ha_leader_kill "leader killed without releasing" sigkill
  eventually "the operator is available again before the next fault" \
    operator_rolled_out || true
  ha_leader_kill "leader deleted gracefully"
}

operator_rolled_out() {
  k "$HUB_CTX" get deployment grid-operator -o json | jq -e '.metadata.generation == .status.observedGeneration
    and .status.replicas == .status.updatedReplicas'
}

# ---------------------------------------------------------------------------
# Self test
# ---------------------------------------------------------------------------
#
# The HA verdicts are awk over monitor logs, so they are checked against
# synthetic logs with no cluster. Run by the HA leg before the e2e.

# want <label> <expected> <got>
want() {
  if [[ $2 == "$3" ]]; then
    pass "selftest $1: $3"
  else
    fail "selftest $1: want $2, got $3"
  fi
}

# refutes <label> <command...>: the assertion under test has to fail here.
refutes() {
  local label=$1 before=$FAILED
  shift
  FAILED=0
  "$@" >/dev/null
  if ((FAILED)); then
    FAILED=$before
    pass "selftest $label: refused"
  else
    FAILED=$before
    fail "selftest $label: accepted what it must refuse"
  fi
}

ttr_of() { measure_ttr selftest t "$1" "$2" | tail -1 | cut -d= -f2; }

cmd_selftest() {
  MODE=selftest
  WORK=$(mktemp -d)
  ARTIFACTS=$WORK/artifacts
  SITE="site-a"

  printf '%s\n' "100 200 site-a" "101 200 site-a" "102 200 site-a" >"$WORK/frontdoor.log"
  want "no fault, no recovery time" 0 "$(ttr_of 100 code)"
  front_door_result "selftest clean window" 100 strict

  printf '%s\n' "100 200 site-a" "101 503 -" "102 503 -" "103 200 site-a" >"$WORK/frontdoor.log"
  want "recovery time spans the outage" 3 "$(ttr_of 100 code)"
  refutes "a 5xx window" front_door_result "selftest outage" 100 strict

  printf '%s\n' "100 200 site-a" "101 000 -" "102 200 site-a" >"$WORK/frontdoor.log"
  refutes "a call that never connected" front_door_result "selftest connects" 100 strict
  front_door_result "selftest connects recorded" 100 lenient

  printf '%s\n' "100 200 hub" "101 200 site-a" >"$WORK/frontdoor.log"
  want "the front door was up throughout" 0 "$(ttr_of 100 code)"
  want "the site came back into the draw" 1 "$(ttr_of 100 draw)"

  printf '%s\n' "100 503 -" "101 503 -" >"$WORK/frontdoor.log"
  want "an outage that never recovered" unrecovered "$(ttr_of 100 code)"

  printf '%s\n' "100 true true" "101 false true" "102 gone gone" >"$WORK/drain.log"
  assert_drain_order "selftest drain order"
  want "a ready EndpointSlice endpoint" true "$(endpoint_status 10.0.0.1 '{"items":[{"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true,"terminating":false}}]}]}')"
  want "an unready EndpointSlice endpoint" false "$(endpoint_status 10.0.0.1 '{"items":[{"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":false,"terminating":false}}]}]}')"
  want "an unready EndpointSlice event" false "$(endpoint_event_status 10.0.0.1 '{"type":"MODIFIED","object":{"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":false,"terminating":false}}]}}')"
  want "an EndpointSlice endpoint with missing readiness" true "$(endpoint_status 10.0.0.1 '{"items":[{"endpoints":[{"addresses":["10.0.0.1"],"conditions":{}}]}]}')"
  want "a terminating EndpointSlice endpoint" false "$(endpoint_status 10.0.0.1 '{"items":[{"endpoints":[{"addresses":["10.0.0.1"],"conditions":{"ready":true,"terminating":true}}]}]}')"
  want "an empty EndpointSlice endpoint set" gone "$(endpoint_status 10.0.0.1 '{"items":[]}')"
  want "a deleted EndpointSlice event" gone "$(endpoint_event_status 10.0.0.1 '{"type":"DELETED","object":{"endpoints":[{"addresses":["10.0.0.1"]}]}}')"
  printf '%s\n' "100 true true" "101 true false" "102 gone gone" >"$WORK/drain.log"
  refutes "a ready endpoint that outlived its container" assert_drain_order "selftest late endpoint"
  : >"$WORK/drain.log"
  refutes "a drain nobody watched" assert_drain_order "selftest unobserved drain"
  if selftest_pod_absent; then
    pass "kubectl lookup errors do not mean a pod is absent"
  else
    fail "pod-absence predicate did not distinguish NotFound from an API error"
  fi

  printf '%s\n' "100 503 0" "101 000 0" "102 200 3" >"$WORK/catalog.log"
  assert_catalog "selftest catalog" 100
  want "the new holder published again" 2 "$(ttr_of 100 catalog)"
  printf '%s\n' "100 200 0" "101 200 3" >"$WORK/catalog.log"
  refutes "a 200 carrying no providers" assert_catalog "selftest empty catalog" 100

  rm -rf "$WORK"
  if ((FAILED)); then
    printf 'RESULT: FAIL (selftest)\n'
    return 1
  fi
  printf 'RESULT: PASS (selftest)\n'
}

# Signed state
# ---------------------------------------------------------------------------

# signed_operator_logs <context> [kubectl logs args...]: every operator pod, ANSI stripped.
signed_operator_logs() {
  local ctx=$1
  shift
  operator_logs "$ctx" "$@" | strip_ansi
}

# unsigned_refused <since>: the hub rejected the site's unsigned state after <since>.
unsigned_refused() {
  local logs
  # Capture first so a matching line cannot cut off the log-fetch pipeline under pipefail.
  logs=$(signed_operator_logs "$HUB_CTX" --since-time="$1")
  grep 'rejecting pinned-origin state broadcast' <<<"$logs" | grep "origin_site=$SITE" | grep missing_signature >/dev/null
}

site_received_signed_probe() {
  k "$SITE_CTX" get configmap grid-overlay-grid-grid-gateway -o json \
    | jq -e '.data["routing-config.json"] | fromjson | any(.candidates[]; .name == "signed-state-probe" and .site == "hub")'
}

# assert_signed_state: under enforce, both operators sign, so the remote candidate
# assert_membership saw came from verified state. Once the site stops signing, the hub
# refuses its state.
assert_signed_state() {
  local ctx logs since
  [[ $SIGNED_STATE == enforce ]] || return 0
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    logs=$(signed_operator_logs "$ctx" --since-time="$SIGNED_FROM")
    if grep -q 'signed gossip disabled' <<<"$logs"; then
      fail "$ctx: operator has no usable signing identity"
    else
      pass "$ctx: operator signs its state"
    fi
    if grep -q 'failed to encode address broadcast' <<<"$logs"; then
      fail "$ctx: operator could not publish its addresses under signed state"
    else
      pass "$ctx: operator publishes its addresses under signed state"
    fi
  done
  eventually "site verifies the hub gateway identity" site_phase_in "$SITE_CTX" grid-hub Active \
    || die "hub gateway identity is not Active on the site"
  helm_on "$HUB_CTX" "$NS" grid-site grid-site "${HUB_SITE_ARGS[@]}" "${SR[@]}" \
    --set inferenceProviders.signed-state-probe.endpoint=http://127.0.0.1:8000 \
    || die "publish a fresh hub provider under signed state"
  eventually "site accepts the named signed provider update from the hub" site_received_signed_probe
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  helm_on "$SITE_CTX" "$NS" grid-site grid-site "${SITE_SITE_ARGS[@]}" \
    --set gridNetwork.peerTrust.signedState=off || die "turn signed state off on the site"
  eventually "hub refuses the site's unsigned state" unsigned_refused "$since" || true
  helm_on "$SITE_CTX" "$NS" grid-site grid-site "${SITE_SITE_ARGS[@]}" || die "turn signed state back on"

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
  local pf="" code try out="$WORK/hubpath" tries=3
  # A port-forward pins one pod. Under the site route leg the gateway rolls on every
  # renewal, so allow a pod swap: Service traffic stays up, this tunnel does not.
  site_route && tries=8
  # stop_monitors kills this subshell; take its port-forward with it.
  # The EXIT trap runs after the function's locals are gone, so read pf defensively.
  trap '[[ -z ${pf:-} ]] || kill "$pf" 2>/dev/null; exit 0' TERM
  trap '[[ -z ${pf:-} ]] || kill "$pf" 2>/dev/null' EXIT
  while :; do
    for ((try = 1; try <= tries; try++)); do
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
  for pid in "$WATCH_PID" "$MTLS_PID" "$HUBPATH_PID" "$FRONTDOOR_PID" "$CATALOG_PID" "$DRAIN_PID"; do
    [[ -z $pid ]] || kill "$pid" 2>/dev/null || true
  done
  wait "$WATCH_PID" "$MTLS_PID" "$HUBPATH_PID" "$FRONTDOOR_PID" "$CATALOG_PID" "$DRAIN_PID" 2>/dev/null || true
  WATCH_PID=""
  MTLS_PID=""
  HUBPATH_PID=""
  FRONTDOOR_PID=""
  CATALOG_PID=""
  DRAIN_PID=""
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
  local target=deployment/grid-operator
  # A candidate polls nothing, so scrape the holder where the operator is elected.
  if [[ $1 == "$HUB_CTX" ]]; then
    local holder
    holder=$(lease_holder)
    [[ -z $holder ]] || target=pod/$holder
  fi
  k "$1" port-forward "$target" "$2:9090" >/dev/null 2>&1 &
  pid=$!
  sleep 2
  curl -sS --max-time 5 --cacert "$WORK/grid-ca.crt" --resolve "$name.grid.internal:$2:127.0.0.1" \
    "https://$name.grid.internal:$2/metrics" \
    | awk -F'[{}]' '/^grid_peer_poll_total\{/ { split($2, l, ","); for (i in l) if (l[i] ~ /^outcome=/) { o = l[i]; gsub(/outcome=|"/, "", o) } ; n[o] += $3 } END { for (o in n) print o "=" n[o] }' \
    | sort || rc=1
  kill "$pid" 2>/dev/null || true
  return "$rc"
}

# operator_logs <context> [kubectl logs args...]: every operator pod. `logs deployment/x` reads
# one pod, which is a coin flip once the operator runs a holder beside a candidate.
operator_logs() {
  local ctx=$1
  shift
  k "$ctx" logs -l app.kubernetes.io/instance=grid-operator --all-containers --prefix \
    --max-log-requests 10 --tail=-1 "$@" 2>/dev/null
}

# renewals_logged <context>: INFO "site identity rotated" lines across this operator's pods.
renewals_logged() {
  operator_logs "$1" | strip_ansi \
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

  # An enrollment admin deletes the enrollment, re-admits the name, and the site enrolls again.
  token=$(admin_token) || { fail "mint a grid-admin token"; return; }
  code=$(enroll_api "$out" DELETE "/enrollments/$SITE" -H @<(printf 'Authorization: Bearer %s\n' "$token"))
  if [[ $code == 204 ]]; then
    pass "grid-admin deleted the $SITE enrollment: 204"
  else
    fail "delete the $SITE enrollment: HTTP $code $(head -c 200 "$out.body" 2>/dev/null)"
    return
  fi
  code=$(jq -cn --arg s "$SITE" '{siteName: $s, gridNetworkRef: "grid", allowDeletedName: true}' \
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

# rotation_reads <context> <status/reason>: the GridNetwork's RotationOnSchedule.
rotation_reads() { [[ $(network_condition "$1" RotationOnSchedule) == "$2" ]]; }

# accepted_reads <context> <status/reason>: the GridNetwork's Accepted.
accepted_reads() { [[ $(network_condition "$1" Accepted) == "$2" ]]; }

# Rotation is declared where the config is generated, never derived at runtime: the chart
# wrote site.signerEndpoint.rotation Disabled under pin into the operator config, and left it
# out under spiffe for the operator's default, Enabled. The GridNetwork's conditions follow:
# RotationOnSchedule reads True either way, and only its message says whether a signer
# renews or nothing does, so the assertion stops at status and reason. Under pin that
# True also says no renewal is scheduled, which status.identity.rotateAfter used to say.
assert_rotation_declared() {
  local ctx declared want
  for ctx in "$HUB_CTX" "$SITE_CTX"; do
    declared=$(k "$ctx" get configmap grid-operator-config -o jsonpath='{.data.config\.yaml}' \
      | sed -n 's/^ *rotation: "\{0,1\}\([A-Za-z]*\)"\{0,1\}$/\1/p')
    want=""
    [[ $TRUST == pin ]] && want=Disabled
    if [[ $declared == "$want" ]]; then
      pass "$ctx: the operator config declares site.signerEndpoint.rotation '${declared:-default Enabled}'"
    else
      fail "$ctx: the operator config declares site.signerEndpoint.rotation '$declared', want '$want'"
    fi
    if [[ $TRUST == pin ]]; then
      eventually "$ctx: GridNetwork reports RotationOnSchedule True with rotation Disabled under pin" \
        rotation_reads "$ctx" True/RotationOnSchedule || true
      eventually "$ctx: GridNetwork is Accepted with rotation Disabled under pin" accepted_reads "$ctx" True/Accepted || true
    else
      eventually "$ctx: GridNetwork reports RotationOnSchedule True through site.signerEndpoint" \
        rotation_reads "$ctx" True/RotationOnSchedule || true
    fi
  done
}

cmd_test() {
  local m tool
  for m in $MODES; do
    case $m in pin | spiffe | local) ;; *) die "MODES entry $m is not pin, spiffe or local" ;; esac
  done
  case $HUB_ROUTER in render | operator) ;; *) die "HUB_ROUTER $HUB_ROUTER is not render or operator" ;; esac
  case $SIGNED_STATE in off | enforce) ;; *) die "SIGNED_STATE $SIGNED_STATE is not off or enforce" ;; esac
  # A renewed key is locked out under enforce: see docs/architecture/signed-gossip-trust.md.
  [[ $SIGNED_STATE == off || -z ${ROTATION_LIFETIME:-} || $MODES != *spiffe* ]] \
    || die "SIGNED_STATE=enforce cannot rotate"
  for tool in kubectl helm jq openssl curl; do
    command -v "$tool" >/dev/null || die "$tool is required"
  done
  MODE=setup
  if ha && [[ -z ${HA_OPERATOR_NODE:-} ]]; then
    HA_OPERATOR_NODE=$(kubectl --context "$HUB_CTX" get nodes \
      -l '!node-role.kubernetes.io/control-plane' -o jsonpath='{.items[0].metadata.name}')
    log "pinning the hub operator to $HA_OPERATOR_NODE"
  fi
  if ha && [[ -z $HA_OPERATOR_REPLICAS ]]; then
    HA_OPERATOR_REPLICAS=$(operator_ha && echo 2 || echo 1)
    log "operator replicas under HA: $HA_OPERATOR_REPLICAS"
  fi
  plan_addresses
  if site_route; then
    model_lb || die "expose the site model to the hub"
    eventually "the site model answers on $MODEL_LB_IP" model_lb_ready || die "no address for the hub-local model"
  fi
  for MODE in $MODES; do
    TRUST=$MODE
    ! local_source || TRUST=spiffe
    REFUSALS_FROM=""
    log "[$MODE] resetting the $NS namespace on both clusters"
    rm -rf "${ARTIFACTS:?}/$MODE"
    reset_grid
    install_grid
    start_renewal_watch
    assert_membership
    assert_serving
    assert_ha
    assert_site_route
    assert_rotation_declared
    assert_renewal
    assert_signed_state
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
  selftest) cmd_selftest ;;
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
