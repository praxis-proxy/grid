#!/usr/bin/env bash
set -euo pipefail

PASS=0
FAIL=0
KIND_CLUSTER=""

OPERATOR_IMAGE="ghcr.io/praxis-proxy/grid-operator"
OPERATOR_TAG="${GRID_OPERATOR_CI_TAG:-v0.1.4}"
DEFAULT_GATEWAY_IMAGE="ghcr.io/praxis-proxy/ai:0.4.0"

# ── Helpers ────────────────────────────────────────────────────────────

pass() { PASS=$((PASS + 1)); echo "  PASS: $1"; }
fail() { FAIL=$((FAIL + 1)); echo "  FAIL: $1" >&2; }

cleanup() {
  if [ -n "$KIND_CLUSTER" ]; then
    echo "Cleaning up Kind cluster $KIND_CLUSTER"
    kind delete cluster --name "$KIND_CLUSTER" 2>/dev/null || true
  fi
  rm -f /tmp/grid-operator-helm-verify-*.tgz
  rm -f /tmp/praxis-gateway-helm-verify-*.tgz
  rm -f /tmp/grid-site-helm-verify-*.tgz
  rm -f /tmp/grid-mock-providers-helm-verify-*.tgz
}
trap cleanup EXIT

# Run a helm template command and report pass/fail.
# Usage: try_template <chart> <label> [helm-args...]
try_template() {
  local chart="$1" label="$2"
  shift 2
  local release
  release=$(echo "v-${label// /-}" | tr '[:upper:]' '[:lower:]' | tr -dc 'a-z0-9-' | head -c 53)
  if helm template "$release" "$chart" "$@" >/dev/null 2>&1; then
    pass "template: $label"
  else
    fail "template: $label"
  fi
}

# render <helm template args...>: stdout into RENDERED. A failed render is a FAIL, never a pass.
render() {
  if RENDERED=$(helm template "$@"); then
    return 0
  fi
  fail "render failed: helm template $*"
  return 1
}

# Run a helm template command and expect failure (schema rejection).
# Usage: try_reject <chart> <label> [helm-args...]
try_reject() {
  local chart="$1" label="$2"
  shift 2
  if helm template "verify-reject" "$chart" "$@" >/dev/null 2>&1; then
    fail "schema should reject: $label"
  else
    pass "schema rejects: $label"
  fi
}

# try_reject_msg <chart> <label> <ERE> <args...>: the render must fail with output matching ERE.
# Match field paths both Helm 3 (a.b.0) and Helm 4 (/a/b/0) print.
try_reject_msg() {
  local chart="$1" label="$2" want="$3" out
  shift 3
  if out=$(helm template "verify-reject" "$chart" "$@" 2>&1 >/dev/null); then
    fail "schema should reject: $label"
  elif grep -qE -- "$want" <<<"$out"; then
    pass "schema rejects: $label"
  else
    fail "schema rejects $label for the wrong reason: $(echo "$out" | head -3 | tr '\n' ' ')"
  fi
}

# ======================================================================
# Grid Operator Chart
# ======================================================================

CHART_DIR="charts/grid-operator"
DEPLOY_CRDS="deploy/crds"

echo "======================================================================"
echo "  Grid Operator Chart ($CHART_DIR)"
echo "======================================================================"

# ── Helm lint ────────────────────────────────────────────────────────
echo ""
echo "=== Helm lint ==="
if helm lint "$CHART_DIR" --strict 2>&1; then
  pass "helm lint --strict"
else
  fail "helm lint --strict"
fi

# ── CRD synchronization ─────────────────────────────────────────────
echo ""
echo "=== CRD synchronization ==="
# Chart CRDs are deploy/crds plus the chart's lifecycle annotations.
crd_body() { yq -o json 'del(.metadata.annotations)' | jq -S .; }
for crd in agenttoolprovider gridnetwork gridsite inferenceprovider; do
  if render v-crds "$CHART_DIR" --show-only "templates/crds/${crd}.yaml" \
    && diff -q <(crd_body <<<"$RENDERED") <(crd_body <"$DEPLOY_CRDS/${crd}.yaml") >/dev/null; then
    pass "crd sync: ${crd}.yaml"
  else
    fail "crd sync: templates/crds/${crd}.yaml differs from $DEPLOY_CRDS/${crd}.yaml"
  fi
done
CRD_TEMPLATES=("$CHART_DIR"/templates/crds/*.yaml)
if [ "${#CRD_TEMPLATES[@]}" -eq 4 ] && [ ! -d "$CHART_DIR/crds" ]; then
  pass "crds ship only as templates gated by crds.enabled"
else
  fail "unexpected CRD files: ${CRD_TEMPLATES[*]} $([ -d "$CHART_DIR/crds" ] && echo "$CHART_DIR/crds")"
fi
if render v-crds "$CHART_DIR" --set crds.enabled=false && ! grep -q 'kind: CustomResourceDefinition' <<<"$RENDERED"; then
  pass "crds.enabled=false renders no CRD"
else
  fail "crds.enabled=false still renders a CRD"
fi

# A CRD missing from the kustomization is silently dropped by kustomize consumers.
listed=$(sed -n 's/^  - //p' "$DEPLOY_CRDS/kustomization.yaml" | sort)
present=$(cd "$DEPLOY_CRDS" && printf '%s\n' *.yaml | grep -vx kustomization.yaml | sort)
if [ "$listed" = "$present" ]; then
  pass "crd kustomization lists every CRD"
else
  fail "crd kustomization out of sync with $DEPLOY_CRDS (rerun scripts/generate-deployment-crds.sh)"
fi

# ── Default template rendering ───────────────────────────────────────
echo ""
echo "=== Template rendering ==="
helm template verify-default "$CHART_DIR" --namespace grid-system > /tmp/helm-rendered-operator.yaml 2>/dev/null || true
try_template "$CHART_DIR" "default values" --namespace grid-system

# ── Variant renderings ──────────────────────────────────────────────
try_template "$CHART_DIR" "digest image" \
  --set image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
try_template "$CHART_DIR" "custom tag" --set image.tag=v1.2.3
try_template "$CHART_DIR" "custom namespace" --namespace custom-ns
try_template "$CHART_DIR" "resource namespaces" \
  --set 'resourceNamespaces={app-ns,data-ns}' --namespace grid-system
try_template "$CHART_DIR" "existing SA no RBAC" \
  --set serviceAccount.create=false --set serviceAccount.name=existing --set rbac.create=false
try_template "$CHART_DIR" "metrics disabled" --set metrics.service.enabled=false
try_template "$CHART_DIR" "ServiceMonitor enabled" \
  --set serviceMonitor.enabled=true --set serviceMonitor.interval=30s
try_template "$CHART_DIR" "SWIM ClusterIP" \
  --set swim.service.enabled=true --set swim.service.type=ClusterIP
try_template "$CHART_DIR" "SWIM LoadBalancer" \
  --set swim.service.enabled=true --set swim.service.type=LoadBalancer \
  --set swim.service.loadBalancerIP=10.0.0.1
SIG_RENDER=$(helm template v-sig "$CHART_DIR" --namespace grid-system --set signals.enabled=true \
  --set swim.service.enabled=true --set swim.service.type=LoadBalancer 2>&1 || true)
if grep -q 'value: "v-sig-grid-operator-swim.grid-system.svc:9091"' <<<"$SIG_RENDER" \
  && grep -A3 -- '- name: signals' <<<"$SIG_RENDER" | grep -q 'targetPort: signals'; then
  pass "signals: TCP port on the SWIM Service and the local gateway address"
else
  fail "signals: unexpected render: $(grep -E 'SIGNALS|signals|Error' <<<"$SIG_RENDER" | head -3 | tr '\n' ' ')"
fi
try_reject_msg "$CHART_DIR" "signals without the SWIM Service" "needs swim.service.enabled" --set signals.enabled=true
# --reuse-values from a release predating these keys leaves them absent.
try_template "$CHART_DIR" "absent signals map" --set signals=null
try_template "$CHART_DIR" "SWIM advertise address" \
  --set swim.service.enabled=true --set swim.service.type=LoadBalancer \
  --set swim.advertiseAddress=swim.example.com:7946
try_template "$CHART_DIR" "scheduling" \
  --set nodeSelector.zone=us-east-1 --set priorityClassName=high-priority
ENROLL_SET=(--set enrollment.enabled=true --set enrollment.url=https://enroll.example.com
  --set enrollment.siteName=site-d --set enrollment.tokenSecretRef.name=grid-invite-site-d)
try_template "$CHART_DIR" "auto-enroll with a ConfigMap CA" "${ENROLL_SET[@]}" \
  --set enrollment.caBundle.configMap=grid-ca
try_template "$CHART_DIR" "auto-enroll with a Secret CA" "${ENROLL_SET[@]}" \
  --set enrollment.caBundle.secret=grid-ca-bundle
for nulled in "" --set=enrollment.caBundle=null; do
  if render v-enroll "$CHART_DIR" "${ENROLL_SET[@]}" ${nulled:+"$nulled"} && grep -q 'secretName: "grid-ca-bundle"' <<<"$RENDERED"; then
    pass "auto-enroll without a CA bundle pins the grid-ca-bundle Secret ${nulled:-(unset)}"
  else
    fail "auto-enroll without a CA bundle did not default to grid-ca-bundle ${nulled:-(unset)}"
  fi
done
try_reject_msg "$CHART_DIR" "auto-enroll site name past 51 characters" 'siteName' "${ENROLL_SET[@]}" \
  --set enrollment.caBundle.secret=grid-ca-bundle --set enrollment.siteName="$(printf 'a%.0s' {1..52})"
try_reject_msg "$CHART_DIR" "auto-enroll over plaintext" 'enrollment[./]url' "${ENROLL_SET[@]}" \
  --set enrollment.caBundle.secret=grid-ca-bundle --set enrollment.url=http://enroll.example.com
try_template "$CHART_DIR" "auto-enroll with a separate grid CA anchor" "${ENROLL_SET[@]}" \
  --set enrollment.caBundle.configMap=route-ca --set enrollment.gridCaBundle.secret=grid-ca-bundle
try_reject_msg "$CHART_DIR" "auto-enroll with two grid CA sources" 'gridCaBundle' "${ENROLL_SET[@]}" \
  --set enrollment.caBundle.configMap=route-ca --set enrollment.gridCaBundle.secret=a \
  --set enrollment.gridCaBundle.configMap=b
try_template "$CHART_DIR" "SA annotations" \
  --set-string 'serviceAccount.annotations.eks\.amazonaws\.com/role-arn=arn:aws:iam::123456789012:role/grid'
try_template "$CHART_DIR" "hostile podLabels" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile'
try_template "$CHART_DIR" "gateway discovery" \
  --set-string gateway.serviceName=edge-gateway --set-string gateway.port=8080

# ── Verify selector protection ──────────────────────────────────────
echo ""
echo "=== Selector protection ==="
RENDERED=$(helm template verify-sel "$CHART_DIR" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile' \
  --namespace grid-system --show-only templates/deployment.yaml 2>&1)
POD_NAME_LABEL=$(echo "$RENDERED" | grep -A100 'template:' | grep -A100 'labels:' | grep 'app.kubernetes.io/name:' | head -1 | awk '{print $2}')
if [ "$POD_NAME_LABEL" = "grid-operator" ]; then
  pass "selector: podLabels cannot override app.kubernetes.io/name"
else
  fail "selector: podLabels overrode app.kubernetes.io/name to '$POD_NAME_LABEL'"
fi

# ── Service link env vars ───────────────────────────────────────────
echo ""
echo "=== Service links ==="
# A Service named grid-gateway injects GRID_GATEWAY_PORT=tcp://..., which clap
# parses as --gateway-port and the operator crashes.
if render verify-links "$CHART_DIR" --namespace grid-system --show-only templates/deployment.yaml; then
  if grep -q 'enableServiceLinks: false' <<<"$RENDERED"; then
    pass "operator pod disables service link env vars"
  else
    fail "operator pod must set enableServiceLinks: false"
  fi
  # Even with service links off, pin the port so no injected value can win.
  if grep -A1 'name: GRID_GATEWAY_PORT' <<<"$RENDERED" | grep -q 'value: "8080"'; then
    pass "operator pod sets GRID_GATEWAY_PORT by default"
  else
    fail "operator pod must set GRID_GATEWAY_PORT by default"
  fi
fi
# Every grid workload pod disables service links, not just the operator.
for spec in "charts/grid-enrollment:3" "charts/grid-mock-providers:1" \
  "charts/praxis-gateway:1:--set-string config.existingConfigMap=verify"; do
  IFS=: read -r chart want extra <<<"$spec"
  # shellcheck disable=SC2086 # extra is a flag list
  render verify-links "$chart" $extra || continue
  got=$(grep -c 'enableServiceLinks: false' <<<"$RENDERED" || true)
  if [ "$got" = "$want" ]; then
    pass "$chart: all $want workload pods disable service links"
  else
    fail "$chart: expected $want pods with enableServiceLinks: false, got $got"
  fi
done

# ── Gateway discovery namespace ─────────────────────────────────────
echo ""
echo "=== Gateway discovery namespace ==="
# Without GRID_GATEWAY_NAMESPACE the operator reads the gateway Service in
# grid-system and a release anywhere else gets a 403.
if render verify-gwns "$CHART_DIR" --namespace release-ns --show-only templates/deployment.yaml; then
  GW_NS=$(grep -A1 'name: GRID_GATEWAY_NAMESPACE' <<<"$RENDERED" | awk '/value:/{print $2}' | tr -d '"')
  if [ "$GW_NS" = "release-ns" ]; then
    pass "gateway namespace defaults to the release namespace"
  else
    fail "gateway namespace: expected release-ns, got '$GW_NS'"
  fi
fi
if render verify-gwns "$CHART_DIR" --namespace release-ns --set-string gateway.namespace=edge-ns; then
  GW_NS=$(grep -A1 'name: GRID_GATEWAY_NAMESPACE' <<<"$RENDERED" | awk '/value:/{print $2}' | tr -d '"')
  if [ "$GW_NS" = "edge-ns" ]; then
    pass "gateway.namespace override sets the env"
  else
    fail "gateway.namespace override: expected edge-ns, got '$GW_NS'"
  fi
  # In the gateway namespace the operator may only get the one gateway Service.
  GW_ROLE=$(yq 'select(.kind == "Role" and .metadata.namespace == "edge-ns") | .rules' -o json <<<"$RENDERED" | jq -c .)
  if [ "$GW_ROLE" = '[{"apiGroups":[""],"resources":["services"],"resourceNames":["provider-gateway"],"verbs":["get"]}]' ]; then
    pass "gateway namespace Role grants only get on the gateway Service"
  else
    fail "gateway namespace Role: unexpected rules '$GW_ROLE'"
  fi
  if yq 'select(.kind == "RoleBinding" and .metadata.namespace == "edge-ns") | .roleRef.kind' <<<"$RENDERED" | grep -qx ClusterRole; then
    fail "gateway namespace binds the resources ClusterRole"
  else
    pass "gateway namespace does not bind the resources ClusterRole"
  fi
fi
if render verify-gwns "$CHART_DIR" --namespace release-ns --set-string gateway.namespace=release-ns; then
  if yq 'select(.kind == "Role") | .metadata.name' <<<"$RENDERED" | grep -q gateway-discovery; then
    fail "gateway namespace Role rendered where the resources Role already applies"
  else
    pass "no gateway namespace Role inside the resource namespaces"
  fi
fi
if render verify-gwns "$CHART_DIR" --namespace release-ns --set-string gateway.namespace=edge-ns \
  --set-string gateway.address=gw.example.com:443; then
  if yq 'select(.kind == "Role") | .metadata.name' <<<"$RENDERED" | grep -q gateway-discovery; then
    fail "gateway namespace Role rendered although gateway.address skips discovery"
  else
    pass "no gateway namespace Role when gateway.address is set"
  fi
fi
try_reject_msg "$CHART_DIR" "gateway.address blank" "gateway.address must not be blank" \
  --set-string gateway.namespace=edge-ns --set-string 'gateway.address=  '
try_reject_msg "$CHART_DIR" "gateway.namespace kube-system" "is a system namespace" --set-string gateway.namespace=kube-system
try_reject_msg "$CHART_DIR" "gateway.namespace openshift-ingress" "is a system namespace" --set-string gateway.namespace=openshift-ingress
try_reject_msg "$CHART_DIR" "gateway.namespace default" "is a system namespace" --set-string gateway.namespace=default
try_reject "$CHART_DIR" "gateway.namespace not DNS-1123" --set-string gateway.namespace=Edge_NS
try_reject_msg "$CHART_DIR" "gateway.serviceName with whitespace" "gateway[./]serviceName" \
  --set-string gateway.namespace=edge-ns --set-string 'gateway.serviceName= edge-gateway '
try_template "$CHART_DIR" "gateway.namespace system with gateway.address" --set-string gateway.namespace=kube-system \
  --set-string gateway.address=gw.example.com:443
try_template "$CHART_DIR" "gateway.namespace system opt-in" --set-string gateway.namespace=kube-system \
  --set gateway.allowSystemNamespace=true

# ── Schema rejection ────────────────────────────────────────────────
echo ""
echo "=== Schema rejection ==="
try_reject "$CHART_DIR" "replicaCount=2" --set replicaCount=2
try_reject "$CHART_DIR" "invalid digest" --set image.digest=invalid
try_reject "$CHART_DIR" "port zero" --set metrics.service.port=0
try_reject "$CHART_DIR" "invalid SWIM type" --set swim.service.type=ExternalName
try_reject "$CHART_DIR" "unknown key" --set typoField=true
try_template "$CHART_DIR" "subchart keys" --set enabled=true --set global.foo=bar

# ── Metrics-dependent resource coherence ────────────────────────────
echo ""
echo "=== Metrics-dependent resources ==="
RENDERED_NO_METRICS=$(helm template verify-nometrics "$CHART_DIR" \
  --set metrics.service.enabled=false --namespace grid-system 2>&1)
if echo "$RENDERED_NO_METRICS" | grep -q 'kind: Pod'; then
  fail "test pod rendered when metrics.service.enabled=false"
else
  pass "test pod omitted when metrics.service.enabled=false"
fi

if helm template verify-smbad "$CHART_DIR" \
  --set serviceMonitor.enabled=true --set metrics.service.enabled=false \
  --namespace grid-system >/dev/null 2>&1; then
  fail "serviceMonitor+noMetrics should fail"
else
  pass "serviceMonitor.enabled fails without metrics service"
fi

# ── Package ──────────────────────────────────────────────────────────
echo ""
echo "=== Helm package ==="
PKG_OUT=$(helm package "$CHART_DIR" -d /tmp 2>&1)
TGZ=$(echo "$PKG_OUT" | grep -oP '/tmp/\S+\.tgz')
if [ -f "$TGZ" ]; then
  pass "helm package: $(basename "$TGZ") ($(stat -c%s "$TGZ") bytes)"
  CONTENTS=$(tar tzf "$TGZ" 2>&1)
  for f in Chart.yaml values.yaml values.schema.json templates/deployment.yaml templates/crds/agenttoolprovider.yaml \
    templates/crds/gridnetwork.yaml templates/crds/gridsite.yaml templates/crds/inferenceprovider.yaml; do
    if echo "$CONTENTS" | grep -q "$f"; then
      pass "package contains: $f"
    else
      fail "package missing: $f"
    fi
  done
  rm -f "$TGZ"
else
  fail "helm package failed"
fi

# ======================================================================
# Praxis Gateway Chart
# ======================================================================

GW_DIR="charts/praxis-gateway"

echo ""
echo "======================================================================"
echo "  Praxis Gateway Chart ($GW_DIR)"
echo "======================================================================"

# Common required argument for the gateway chart. The image intentionally uses
# the chart default so this path validates the official Praxis AI contract.
GW_REQ=(--set config.existingConfigMap=test-config)

# ── Helm lint ────────────────────────────────────────────────────────
echo ""
echo "=== Helm lint ==="
if helm lint "$GW_DIR" --strict "${GW_REQ[@]}" 2>&1; then
  pass "helm lint --strict (gateway)"
else
  fail "helm lint --strict (gateway)"
fi
# The release workflow lints with no values, so the standalone default must lint clean.
if helm lint "$GW_DIR" --strict 2>&1; then
  pass "helm lint --strict (gateway, no values)"
else
  fail "helm lint --strict (gateway, no values)"
fi

# ── Default template rendering ───────────────────────────────────────
echo ""
echo "=== Template rendering ==="
helm template verify-default "$GW_DIR" "${GW_REQ[@]}" --namespace grid-system > /tmp/helm-rendered-gateway.yaml 2>/dev/null || true
try_template "$GW_DIR" "gateway default" "${GW_REQ[@]}" --namespace grid-system
if grep -Fq "image: ${DEFAULT_GATEWAY_IMAGE}" /tmp/helm-rendered-gateway.yaml; then
  pass "gateway default image: ${DEFAULT_GATEWAY_IMAGE}"
else
  fail "gateway default image is not ${DEFAULT_GATEWAY_IMAGE}"
fi

# ── Variant renderings ──────────────────────────────────────────────
try_template "$GW_DIR" "edge gateway" "${GW_REQ[@]}" \
  --set nameOverride=edge-gateway \
  --set service.type=LoadBalancer \
  --set overlay.enabled=true --set overlay.existingConfigMap=grid-overlay \
  --set tls.enabled=true --set tls.existingSecret=edge-tls
try_template "$GW_DIR" "provider gateway" "${GW_REQ[@]}" \
  --set nameOverride=provider-gateway \
  --set port.containerPort=8443 --set port.name=https-mtls \
  --set service.type=LoadBalancer --set service.port=8443 \
  --set tls.enabled=true --set tls.existingSecret=provider-tls
try_template "$GW_DIR" "gtm emulator" "${GW_REQ[@]}" \
  --set nameOverride=gtm-emulator \
  --set port.containerPort=8443 --set port.name=https \
  --set service.type=LoadBalancer --set service.port=8443 \
  --set tls.enabled=true --set tls.existingSecret=gtm-tls
try_template "$GW_DIR" "service disabled" "${GW_REQ[@]}" --set service.enabled=false
try_template "$GW_DIR" "custom image" "${GW_REQ[@]}" \
  --set image.repository=praxis-ai --set image.tag=glb-demo --set image.pullPolicy=Never
try_template "$GW_DIR" "gateway with credentials" "${GW_REQ[@]}" \
  --set 'credentials[0].name=cred-a' --set 'credentials[0].mountPath=/etc/praxis/credentials/a' \
  --set 'credentials[1].name=cred-b' --set 'credentials[1].mountPath=/etc/praxis/credentials/b' \
  --set 'credentials[1].optional=true'
try_template "$GW_DIR" "hostile podLabels gateway" "${GW_REQ[@]}" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile'

# ── Example values rendering ────────────────────────────────────────
echo ""
echo "=== Example values rendering ==="
EXAMPLE_DIR="examples/helm/existing-clusters"

for f in "$EXAMPLE_DIR"/dedicated-edge/values/*-operator.yaml; do
  LABEL="example dedicated-edge $(basename "$f" .yaml)"
  try_template "$CHART_DIR" "$LABEL" --namespace grid-system -f "$f"
done

for f in "$EXAMPLE_DIR"/dedicated-edge/values/*-gateway.yaml; do
  LABEL="example dedicated-edge $(basename "$f" .yaml)"
  try_template "$GW_DIR" "$LABEL" "${GW_REQ[@]}" --namespace grid-system -f "$f"
done

for f in "$EXAMPLE_DIR"/combined-site/values/*-operator.yaml; do
  LABEL="example combined-site $(basename "$f" .yaml)"
  try_template "$CHART_DIR" "$LABEL" --namespace grid-system -f "$f"
done

for f in "$EXAMPLE_DIR"/combined-site/values/*-consumer-gateway.yaml "$EXAMPLE_DIR"/combined-site/values/*-provider-gateway.yaml; do
  LABEL="example combined-site $(basename "$f" .yaml)"
  try_template "$GW_DIR" "$LABEL" "${GW_REQ[@]}" --namespace grid-system -f "$f"
done

for f in "$EXAMPLE_DIR"/combined-site/values/*-grid-site.yaml; do
  LABEL="example combined-site $(basename "$f" .yaml)"
  try_template "charts/grid-site" "$LABEL" --namespace grid-system -f "$f"
done

for f in "$EXAMPLE_DIR"/combined-site/values/*-grid-mock-providers.yaml; do
  LABEL="example combined-site $(basename "$f" .yaml)"
  try_template "charts/grid-mock-providers" "$LABEL" --namespace grid-system -f "$f"
done

# Hub and site example: each values file as the README installs it.
HS_VALUES="examples/helm/hub-site/values"
HS_DIGEST=$(printf 'a%.0s' $(seq 64))
try_template "charts/grid-enrollment" "example hub-site hub-grid-enrollment" --namespace grid \
  -f "$HS_VALUES/hub-grid-enrollment.yaml"
try_template "charts/grid-enrollment" "example hub-site hub-grid-enrollment route" --namespace grid \
  -f "$HS_VALUES/hub-grid-enrollment.yaml" --set route.enabled=true --set-string route.host=enroll.apps.example.com
# The digest placeholders fail closed until replaced.
HS_PINNED=(--set "peers.hub.digest=$HS_DIGEST")
HS_GW_PINNED=(--set "gatewayConfig.peerTrust.digest=$HS_DIGEST")
for side in hub site; do
  try_template "$CHART_DIR" "example hub-site $side-grid-operator" --namespace grid -f "$HS_VALUES/$side-grid-operator.yaml"
done
try_template "charts/grid-site" "example hub-site hub-grid-site" --namespace grid -f "$HS_VALUES/hub-grid-site.yaml"
try_template "charts/grid-site" "example hub-site site-grid-site" --namespace grid -f "$HS_VALUES/site-grid-site.yaml" \
  "${HS_PINNED[@]}"
try_template "$GW_DIR" "example hub-site hub-praxis-gateway" --namespace grid -f "$HS_VALUES/hub-praxis-gateway.yaml"
try_template "$GW_DIR" "example hub-site site-praxis-gateway" --namespace grid -f "$HS_VALUES/site-praxis-gateway.yaml" \
  "${HS_GW_PINNED[@]}"
try_template "$GW_DIR" "example hub-site site-praxis-gateway spiffe" --namespace grid \
  -f "$HS_VALUES/site-praxis-gateway.yaml" --set gatewayConfig.peerTrust.mode=spiffe --set gatewayConfig.peerTrust.digest="" \
  --set gatewayConfig.peerTrust.spiffeId=spiffe://grid.internal/site/hub
try_reject_msg "$GW_DIR" "example hub-site site gateway with the digest placeholder" "digest" --namespace grid \
  -f "$HS_VALUES/site-praxis-gateway.yaml"
try_reject_msg "charts/grid-site" "example hub-site site grid-site with the digest placeholder" "digest" \
  --namespace grid -f "$HS_VALUES/site-grid-site.yaml"

# ── Verify fullnameOverride ──────────────────────────────────────────
echo ""
echo "=== fullnameOverride (gateway) ==="
DEFAULT_SVC_NAME=$(helm template consumer-gateway "$GW_DIR" "${GW_REQ[@]}" \
  --namespace grid-system --show-only templates/service.yaml 2>/dev/null \
  | grep 'name:' | head -1 | awk '{print $2}')
if [ "$DEFAULT_SVC_NAME" = "consumer-gateway-praxis-gateway" ]; then
  pass "fullname: default is {release}-praxis-gateway"
else
  fail "fullname: expected consumer-gateway-praxis-gateway, got '$DEFAULT_SVC_NAME'"
fi

OVERRIDE_SVC_NAME=$(helm template consumer-gateway "$GW_DIR" "${GW_REQ[@]}" \
  --set fullnameOverride=consumer-gateway \
  --namespace grid-system --show-only templates/service.yaml 2>/dev/null \
  | grep 'name:' | head -1 | awk '{print $2}')
if [ "$OVERRIDE_SVC_NAME" = "consumer-gateway" ]; then
  pass "fullname: fullnameOverride produces exact name"
else
  fail "fullname: expected consumer-gateway, got '$OVERRIDE_SVC_NAME'"
fi

# ── Verify selector protection ──────────────────────────────────────
echo ""
echo "=== Selector protection (gateway) ==="
RENDERED=$(helm template verify-gw-sel "$GW_DIR" "${GW_REQ[@]}" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile' \
  --namespace grid-system --show-only templates/deployment.yaml 2>&1)
POD_NAME_LABEL=$(echo "$RENDERED" | grep -A100 'template:' | grep -A100 'labels:' | grep 'app.kubernetes.io/name:' | head -1 | awk '{print $2}')
if [ "$POD_NAME_LABEL" = "praxis-gateway" ]; then
  pass "selector: gateway podLabels cannot override app.kubernetes.io/name"
else
  fail "selector: gateway podLabels overrode app.kubernetes.io/name to '$POD_NAME_LABEL'"
fi

# ── Schema rejection ────────────────────────────────────────────────
echo ""
echo "=== Schema rejection (gateway) ==="
try_template "$GW_DIR" "standalone default (no values)" --namespace praxis
try_reject_msg "$GW_DIR" "blank config.inline" "config.inline is empty" --set-string 'config.inline= ' --namespace praxis
try_reject_msg "$GW_DIR" "config.inline not a mapping" "config.inline is not a valid YAML mapping" \
  --set-string config.inline=not-a-mapping --namespace praxis
try_reject "$GW_DIR" "invalid digest (gw)" "${GW_REQ[@]}" --set image.digest=invalid
try_reject "$GW_DIR" "invalid service type (gw)" "${GW_REQ[@]}" --set service.type=ExternalName
try_reject "$GW_DIR" "unknown key (gw)" "${GW_REQ[@]}" --set typoField=true
try_template "$GW_DIR" "subchart keys (gw)" "${GW_REQ[@]}" --set enabled=true --set global.foo=bar
try_reject "$GW_DIR" "runAsNonRoot override" "${GW_REQ[@]}" --set podSecurityContext.runAsNonRoot=false
try_reject "$GW_DIR" "overlay enabled no name" "${GW_REQ[@]}" --set overlay.enabled=true
try_reject "$GW_DIR" "tls enabled no secret" "${GW_REQ[@]}" --set tls.enabled=true --set tls.existingSecret=""

# ── Secure gateway config (render) ──────────────────────────────────
GW_RENDER=(--set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.model=q --set gatewayConfig.auth.mode=none
  --set "gatewayConfig.backends[0].cluster=a" --set "gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000"
  --set "gatewayConfig.backends[0].transport.mode=plaintext")
echo ""
echo "=== Secure gateway config (gateway) ==="
SECURE_ARGS=(
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub
  --set gatewayConfig.model=qwen3
  --set gatewayConfig.auth.mode=api-key --set image.tag=verify-api-key
  --set gatewayConfig.auth.validateUrl=https://maas-api.svc:8443/internal/v1/api-keys/validate
  --set gatewayConfig.upstreamCA.secretName=upstream-ca
  --set tls.enabled=true --set tls.existingSecret=grid-identity
  --set gatewayConfig.listenerTls.enabled=true --set gatewayConfig.listenerTls.existingSecret=listener-cert
  --set "gatewayConfig.backends[0].cluster=site-a"
  --set "gatewayConfig.backends[0].endpoints[0]=172.30.202.42:8000"
  --set "gatewayConfig.backends[0].transport.sni=site-a.grid.internal" --set "gatewayConfig.backends[0].site=site-a"
  --set "gatewayConfig.backends[1].cluster=site-b"
  --set "gatewayConfig.backends[1].endpoints[0]=172.30.181.254:8000"
  --set "gatewayConfig.backends[1].transport.mode=plaintext"
)
SECURE_RENDER=$(helm template verify-secure "$GW_DIR" "${SECURE_ARGS[@]}" --namespace grid-system 2>&1)

if echo "$SECURE_RENDER" | grep -q 'insecure_options'; then
  fail "secure config: insecure_options must never be emitted"
else
  pass "secure config: no insecure_options"
fi
if echo "$SECURE_RENDER" | grep -q 'address: "127.0.0.1:9901"'; then
  pass "secure config: admin bound to 127.0.0.1"
else
  fail "secure config: admin not bound to 127.0.0.1"
fi
if echo "$SECURE_RENDER" | grep -q 'upstream_ca_file: "/etc/praxis/upstream-ca/ca.crt"'; then
  pass "secure config: upstream_ca_file set from upstreamCA mount"
else
  fail "secure config: upstream_ca_file missing"
fi
# api-key strips the caller's key before any upstream filter.
if echo "$SECURE_RENDER" | awk '/filter: policy/{p=1} p&&/request_remove: \[Authorization\]/{r=1} r&&/filter: load_balancer/{print "ok"; exit}' | grep -q ok; then
  pass "secure config: api-key strips Authorization before load_balancer"
else
  fail "secure config: Authorization not stripped before load_balancer"
fi
if echo "$SECURE_RENDER" | grep -q 'trusted_private_endpoints'; then
  fail "secure config: trusted_private_endpoints is not a Praxis 0.7.0 policy field"
else
  pass "secure config: no trusted_private_endpoints"
fi
NONE_RENDER=$(helm template verify-none "$GW_DIR" "${GW_RENDER[@]}" --namespace grid-system)
if echo "$NONE_RENDER" | grep -qE 'filter: policy|policy.yaml'; then
  fail "secure config: auth.mode none must render no policy"
else
  pass "secure config: auth.mode none renders no policy"
fi
if echo "$NONE_RENDER" | grep -q 'request_remove: \[Authorization\]'; then
  pass "secure config: auth.mode none still strips Authorization by default"
else
  fail "secure config: auth.mode none should strip Authorization by default"
fi
CA_RENDER=$(helm template verify-ca "$GW_DIR" "${SECURE_ARGS[@]}" --namespace grid-system \
  --set gatewayConfig.auth.validateCA.configMap=service-ca --set gatewayConfig.auth.validateCA.key=service-ca.crt)
if echo "$CA_RENDER" | grep -A1 'name: SSL_CERT_FILE' | grep -q '/etc/praxis/validate-ca/service-ca.crt' \
    && echo "$CA_RENDER" | grep -q 'mountPath: "/etc/praxis/validate-ca"'; then
  pass "secure config: validateCA mounts the bundle and sets SSL_CERT_FILE"
else
  fail "secure config: validateCA should mount the bundle and set SSL_CERT_FILE"
fi
NP_GW=$(helm template verify-np "$GW_DIR" "${GW_RENDER[@]}" --set networkPolicy.enabled=true \
  --set-json 'networkPolicy.from=[{"podSelector":{"matchLabels":{"app":"front"}}}]' \
  --show-only templates/networkpolicy.yaml --namespace grid-system 2>&1)
if echo "$NP_GW" | grep -q 'kind: NetworkPolicy' && echo "$NP_GW" | grep -q 'app: front' \
    && echo "$NP_GW" | grep -q 'port: 8080'; then
  pass "networkPolicy: limits listener ingress to the listed peers"
else
  fail "networkPolicy: should limit listener ingress to the listed peers"
fi
try_template "$GW_DIR" "none + LoadBalancer with allowUnauthenticatedExposure (gw)" "${GW_RENDER[@]}" \
  --set service.type=LoadBalancer --set gatewayConfig.auth.allowUnauthenticatedExposure=true --namespace grid-system
if echo "$SECURE_RENDER" | grep -q 'sni: "site-a.grid.internal"' && echo "$SECURE_RENDER" | grep -q 'verify: true'; then
  pass "secure config: mutual_tls backend renders sni + verify:true"
else
  fail "secure config: mutual_tls backend missing sni/verify"
fi
# site-b is plaintext and last: the first tls: after its stanza must not exist.
if echo "$SECURE_RENDER" | awk '/- name: "site-b"/{f=1} f&&/[^-] tls:/{print; exit}' | grep -q 'tls:'; then
  fail "secure config: plaintext backend must not render a tls block"
else
  pass "secure config: plaintext backend renders no tls block"
fi
if echo "$SECURE_RENDER" | grep -q 'cert_path: "/etc/praxis/listener-tls/tls.crt"'; then
  pass "secure config: listenerTls renders listener certificates"
else
  fail "secure config: listenerTls certificates missing"
fi
# mutual_tls backend must probe over tcp (the active http probe is plaintext, unusable to a TLS peer).
if echo "$SECURE_RENDER" | awk '/- name: "site-a"/{f=1} f&&/type:/{print; exit}' | grep -q 'type: "tcp"'; then
  pass "secure config: mutual_tls backend health_check defaults to tcp"
else
  fail "secure config: mutual_tls backend health_check should default to tcp"
fi
if echo "$SECURE_RENDER" | awk '/- name: "site-b"/{f=1} f&&/type:/{print; exit}' | grep -q 'type: "http"'; then
  pass "secure config: plaintext backend health_check defaults to http"
else
  fail "secure config: plaintext backend health_check should default to http"
fi

try_reject_msg "$GW_DIR" "http validateUrl (gw)" "https://" "${GW_RENDER[@]}" \
  --set image.tag=verify-api-key --set gatewayConfig.auth.mode=api-key --set gatewayConfig.auth.validateUrl=http://maas/validate --namespace grid-system
try_reject_msg "$GW_DIR" "api-key without validateUrl (gw)" "validateUrl is required" "${GW_RENDER[@]}" \
  --set gatewayConfig.auth.mode=api-key --namespace grid-system
try_reject_msg "$GW_DIR" "render without auth.mode (gw)" "auth.mode is required" \
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.model=q --set "gatewayConfig.backends[0].cluster=a" \
  --set "gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000" --namespace grid-system
try_reject_msg "$GW_DIR" "none + LoadBalancer (gw)" "exposes unauthenticated inference" "${GW_RENDER[@]}" --set service.type=LoadBalancer --namespace grid-system
try_reject_msg "$GW_DIR" "none + NodePort (gw)" "exposes unauthenticated inference" "${GW_RENDER[@]}" --set service.type=NodePort --namespace grid-system
try_reject_msg "$GW_DIR" "validateCA configMap and secret (gw)" "set configMap or secret, not both" "${SECURE_ARGS[@]}" \
  --set gatewayConfig.auth.validateCA.configMap=a --set gatewayConfig.auth.validateCA.secret=b --namespace grid-system
try_reject_msg "$GW_DIR" "networkPolicy enabled without from (gw)" "needs at least one peer" "${GW_REQ[@]}" \
  --set networkPolicy.enabled=true --namespace grid-system
try_reject_msg "$GW_DIR" "networkPolicy from an empty namespaceSelector (gw)" "admits every pod in every namespace" "${GW_REQ[@]}" \
  --set networkPolicy.enabled=true --set-json 'networkPolicy.from=[{"namespaceSelector":{},"podSelector":{}}]' \
  --namespace grid-system
try_reject_msg "$GW_DIR" "networkPolicy from ipBlock ::/0 (gw)" "admits every address" "${GW_REQ[@]}" \
  --set networkPolicy.enabled=true --set-json 'networkPolicy.from=[{"ipBlock":{"cidr":"::/0"}}]' \
  --namespace grid-system
BK1=(--set "gatewayConfig.backends[0].cluster=a" --set "gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000"
  --set "gatewayConfig.backends[0].transport.mode=plaintext")
R0=(--set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.model=q --set gatewayConfig.auth.mode=none --namespace grid-system)
try_reject_msg "$GW_DIR" "backend without cluster (gw)" "backends[./]0.*cluster" "${R0[@]}" \
  --set "gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000"
try_reject_msg "$GW_DIR" "backend without endpoints (gw)" "backends[./]0.*endpoints" "${R0[@]}" \
  --set "gatewayConfig.backends[0].cluster=a"
try_reject_msg "$GW_DIR" "duplicate backend cluster (gw)" "listed twice" "${R0[@]}" "${BK1[@]}" \
  --set "gatewayConfig.backends[1].cluster=a" --set "gatewayConfig.backends[1].endpoints[0]=1.2.3.5:8000" \
  --set "gatewayConfig.backends[1].transport.mode=plaintext"
try_reject_msg "$GW_DIR" "blank localSite (gw)" "localSite" "${R0[@]}" "${BK1[@]}" --set gatewayConfig.localSite=""
try_reject_msg "$GW_DIR" "blank model (gw)" "gatewayConfig.model is required" "${R0[@]}" "${BK1[@]}" --set-string "gatewayConfig.model= "
try_reject_msg "$GW_DIR" "unknown healthCheck key (gw)" "healthCheck" "${R0[@]}" "${BK1[@]}" \
  --set "gatewayConfig.backends[0].healthCheck.bogus=1"
try_reject_msg "$GW_DIR" "api-key on the default image (gw)" "unsupported on the default image" "${R0[@]}" "${BK1[@]}" \
  --set gatewayConfig.auth.mode=api-key --set gatewayConfig.auth.validateUrl=https://maas/validate
try_reject_msg "$GW_DIR" "api-key on the default image via an empty tag (gw)" "unsupported on the default image" "${R0[@]}" "${BK1[@]}" \
  --set image.tag="" --set gatewayConfig.auth.mode=api-key --set gatewayConfig.auth.validateUrl=https://maas/validate
try_reject_msg "$GW_DIR" "validateUrl without a host (gw)" "validateUrl" "${R0[@]}" "${BK1[@]}" \
  --set image.tag=verify-api-key --set gatewayConfig.auth.mode=api-key --set gatewayConfig.auth.validateUrl=https:///v
try_reject_msg "$GW_DIR" "blank backend endpoint (gw)" "endpoints[./]0" "${R0[@]}" --set "gatewayConfig.backends[0].cluster=a" \
  --set-string "gatewayConfig.backends[0].endpoints[0]= " --set "gatewayConfig.backends[0].transport.mode=plaintext"
try_reject_msg "$GW_DIR" "networkPolicy from an empty peer (gw)" "empty peer" "${GW_REQ[@]}" \
  --set networkPolicy.enabled=true --set-json 'networkPolicy.from=[{}]' --namespace grid-system
try_template "$GW_DIR" "networkPolicy from all addresses with except (gw)" "${GW_REQ[@]}" --set networkPolicy.enabled=true \
  --set-json 'networkPolicy.from=[{"ipBlock":{"cidr":"0.0.0.0/0","except":["10.0.0.0/8"]}}]' --namespace grid-system
try_reject_msg "$GW_DIR" "api-key validateUrl IP literal (gw)" "not an IP address" "${R0[@]}" "${BK1[@]}" \
  --set image.tag=verify-api-key --set gatewayConfig.auth.mode=api-key --set gatewayConfig.auth.validateUrl=https://10.0.0.1:8443/v
try_reject_msg "$GW_DIR" "networkPolicy from ipBlock 0.0.0.0/0 (gw)" "admits every address" "${GW_REQ[@]}" \
  --set networkPolicy.enabled=true --set-json 'networkPolicy.from=[{"ipBlock":{"cidr":"0.0.0.0/0"}}]' --namespace grid-system
try_reject_msg "$GW_DIR" "networkPolicy from a bare namespaceSelector (gw)" "admits every pod in every namespace" "${GW_REQ[@]}" \
  --set networkPolicy.enabled=true --set-json 'networkPolicy.from=[{"namespaceSelector":{}}]' --namespace grid-system
if helm template v-hc "$GW_DIR" "${R0[@]}" "${BK1[@]}" --set "gatewayConfig.backends[0].healthCheck.type=tcp" \
    --show-only templates/gateway-config.yaml | awk '/health_check:/{f=1} f&&/path:/{print; exit}' | grep -q path; then
  fail "tcp health_check should carry no path"
else
  pass "tcp health_check carries no path"
fi
if [ "$(helm template v-ca "$GW_DIR" "${R0[@]}" "${BK1[@]}" --set gatewayConfig.auth.validateCA.configMap=x | grep -c 'SSL_CERT_FILE')" = 0 ]; then
  pass "validateCA is ignored outside api-key"
else
  fail "validateCA should apply only with api-key"
fi
if helm template v-probe "$GW_DIR" "${GW_REQ[@]}" --set health.readiness.httpGet.path=/ --set health.readiness.httpGet.port=http \
    --show-only templates/deployment.yaml --namespace grid-system | sed -n '/readinessProbe/,/livenessProbe/p' | grep -q tcpSocket; then
  fail "an httpGet readiness probe should drop the default tcpSocket"
else
  pass "an httpGet readiness probe drops the default tcpSocket"
fi
try_reject_msg "$GW_DIR" "render without model (gw)" "gatewayConfig.model is required" \
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --set gatewayConfig.backends[0].cluster=a \
  --set gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000 --namespace grid-system
try_reject_msg "$GW_DIR" "render without backends (gw)" "gatewayConfig.backends needs at least one backend" \
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --set gatewayConfig.model=q --namespace grid-system
try_reject_msg "$GW_DIR" "mutual_tls without sni (gw)" "sets no transport.sni" \
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --set gatewayConfig.model=q \
  --set tls.enabled=true --set tls.existingSecret=id \
  --set gatewayConfig.backends[0].cluster=a --set gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000 --namespace grid-system
try_reject_msg "$GW_DIR" "mutual_tls without grid identity (gw)" "tls.enabled is false" \
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --set gatewayConfig.model=q \
  --set gatewayConfig.backends[0].cluster=a --set gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000 \
  --set gatewayConfig.backends[0].transport.mode=mutual_tls --set gatewayConfig.backends[0].transport.sni=a.grid --namespace grid-system
try_reject_msg "$GW_DIR" "plaintext with sni (gw)" "sni belongs to a TLS transport" \
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --set gatewayConfig.model=q \
  --set gatewayConfig.backends[0].cluster=a --set gatewayConfig.backends[0].endpoints[0]=1.2.3.4:8000 \
  --set gatewayConfig.backends[0].transport.mode=plaintext --set gatewayConfig.backends[0].transport.sni=x --namespace grid-system
# tls transport: server-verified backend with no client cert (a KServe workload).
TLS1=(--set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --set gatewayConfig.model=q
  --set "gatewayConfig.backends[0].cluster=kserve" --set "gatewayConfig.backends[0].transport.mode=tls" --namespace grid-system)
TLS_RENDER=$(helm template v-tls "$GW_DIR" "${TLS1[@]}" --set "gatewayConfig.backends[0].endpoints[0]=172.30.1.2:8000" \
  --set "gatewayConfig.backends[0].transport.sni=qwen3-kserve-workload-svc.llm.svc" \
  --set "gatewayConfig.backends[0].transport.ca.configMap=openshift-service-ca.crt" \
  --set "gatewayConfig.backends[0].transport.ca.key=service-ca.crt" 2>&1)
if echo "$TLS_RENDER" | grep -q 'ca_path: "/etc/praxis/backend-ca/0/service-ca.crt"' \
    && echo "$TLS_RENDER" | grep -q 'sni: "qwen3-kserve-workload-svc.llm.svc"' \
    && ! echo "$TLS_RENDER" | awk '/- name: "kserve"/{f=1} f&&/client_cert/{print; exit}' | grep -q client_cert \
    && echo "$TLS_RENDER" | grep -A2 'name: backend-ca-0' | grep -q 'name: "openshift-service-ca.crt"'; then
  pass "tls backend: server-verified with transport.ca and sni, no client cert"
else
  fail "tls backend: should render ca_path, sni, verify, no client_cert, and mount the CA"
fi
if echo "$TLS_RENDER" | awk '/- name: "kserve"/{f=1} f&&/type:/{print; exit}' | grep -q 'type: "tcp"'; then
  pass "tls backend: health_check defaults to tcp"
else
  fail "tls backend: health_check should default to tcp"
fi
try_reject_msg "$GW_DIR" "tls backend: IP endpoint without sni (gw)" "without transport.sni" "${TLS1[@]}" \
  --set "gatewayConfig.backends[0].endpoints[0]=172.30.1.2:8000"
try_reject_msg "$GW_DIR" "tls backend: ca with configMap and secret (gw)" "transport[./]ca.*oneOf" "${TLS1[@]}" \
  --set "gatewayConfig.backends[0].endpoints[0]=172.30.1.2:8000" --set "gatewayConfig.backends[0].transport.sni=h" \
  --set "gatewayConfig.backends[0].transport.ca.configMap=a" --set "gatewayConfig.backends[0].transport.ca.secret=b"
try_reject_msg "$GW_DIR" "tls backend: empty ca (gw)" "transport[./]ca.*oneOf" "${TLS1[@]}" \
  --set "gatewayConfig.backends[0].endpoints[0]=172.30.1.2:8000" --set "gatewayConfig.backends[0].transport.sni=h" \
  --set-json 'gatewayConfig.backends[0].transport.ca={}'
try_reject_msg "$GW_DIR" "transport.ca outside tls (gw)" "transport/mode': value must be 'tls'|transport: Must validate \"then\"" "${TLS1[@]}" \
  --set "gatewayConfig.backends[0].endpoints[0]=172.30.1.2:8000" --set "gatewayConfig.backends[0].transport.mode=plaintext" \
  --set "gatewayConfig.backends[0].transport.ca.configMap=a"

# trustPrivate: the FQDN endpoint and SNI drop the root dot.
TRUST_RENDER=$(helm template v-trust "$GW_DIR" "${TLS1[@]}" \
  --set "gatewayConfig.backends[0].endpoints[0]=m.ns.svc.cluster.local.:8000" --set "gatewayConfig.backends[0].trustPrivate=true" 2>&1 || true)
if grep -q '"m.ns.svc.cluster.local"$' <<<"$TRUST_RENDER" && grep -q 'sni: "m.ns.svc.cluster.local"' <<<"$TRUST_RENDER"; then
  pass "trustPrivate: lists the FQDN without its root dot and derives an undotted sni"
else
  fail "trustPrivate: want the undotted trust entry and sni, got: $(grep -E 'sni:|trusted|svc' <<<"$TRUST_RENDER" | head -3 | tr '\n' ' ')"
fi
try_reject_msg "$GW_DIR" "trustPrivate with only IP endpoints (gw)" "no hostname endpoint" "${TLS1[@]}" \
  --set "gatewayConfig.backends[0].endpoints[0]=172.30.1.2:8000" --set "gatewayConfig.backends[0].transport.sni=h" --set "gatewayConfig.backends[0].trustPrivate=true"
try_reject_msg "$GW_DIR" "trustPrivate over plaintext without allowPlaintextTrust (gw)" "allowPlaintextTrust" \
  --set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --set gatewayConfig.model=q --namespace grid-system \
  --set "gatewayConfig.backends[0].cluster=p" --set "gatewayConfig.backends[0].transport.mode=plaintext" \
  --set "gatewayConfig.backends[0].endpoints[0]=m.ns.svc.cluster.local.:8000" --set "gatewayConfig.backends[0].trustPrivate=true"

# provider role: serves the grid identity, requires a client cert, routes to one local backend.
DIGEST=$(printf 'a%.0s' $(seq 64))
PROVIDER=(--set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.role=provider --namespace grid-system
  --set image.flavor=grid-gateway
  --set tls.enabled=true --set tls.existingSecret=grid-site-identity --set tls.caSecret=grid-ca
  --set "gatewayConfig.backends[0].cluster=local" --set "gatewayConfig.backends[0].transport.mode=plaintext"
  --set "gatewayConfig.backends[0].endpoints[0]=m.ns.svc.cluster.local.:8000" --set "gatewayConfig.backends[0].trustPrivate=true"
  --set "gatewayConfig.backends[0].allowPlaintextTrust=true" --set-json "gatewayConfig.peerTrust.certDigests=[\"$DIGEST\"]")
SPIFFE=(--set gatewayConfig.peerTrust.mode=spiffe --set gatewayConfig.peerTrust.certDigests=null)
PROV_RENDER=$(helm template v-prov "$GW_DIR" "${PROVIDER[@]}" 2>&1 || true)
if grep -q 'client_cert_mode: require$' <<<"$PROV_RENDER" && grep -q "cert_digest: \"$DIGEST\"" <<<"$PROV_RENDER" \
  && grep -q 'name: "grid-ca"' <<<"$PROV_RENDER" && ! grep -q 'intelligent_route' <<<"$PROV_RENDER"; then
  pass "provider pin: Grid-CA client auth plus the certificate digest allowlist"
else
  fail "provider pin: unexpected render: $(grep -E 'client_cert_mode|cert_digest|Error' <<<"$PROV_RENDER" | head -3 | tr '\n' ' ')"
fi
if grep -q -- '- path: "/v1/chat/completions"' <<<"$PROV_RENDER" && ! grep -q 'path_prefix' <<<"$PROV_RENDER"; then
  pass "provider: routes only the allowed inference paths"
else
  fail "provider: routes are not limited to allowedPaths"
fi
SPIFFE_RENDER=$(helm template v-prov "$GW_DIR" "${PROVIDER[@]}" "${SPIFFE[@]}" \
  --set "gatewayConfig.peerTrust.spiffeIds[0]=spiffe://grid.internal/site/hub" 2>&1 || true)
if grep -q 'client_cert_mode: require_named$' <<<"$SPIFFE_RENDER" && grep -q -- '- "spiffe://grid.internal/site/hub"' <<<"$SPIFFE_RENDER" \
  && ! grep -q 'peer_identity_trust' <<<"$SPIFFE_RENDER"; then
  pass "provider spiffe: require_named with the SPIFFE allowlist and no pins"
else
  fail "provider spiffe: unexpected render: $(grep -E 'client_cert_mode|spiffe|Error' <<<"$SPIFFE_RENDER" | head -3 | tr '\n' ' ')"
fi
if helm template v-prov "$GW_DIR" "${PROVIDER[@]}" "${SPIFFE[@]}" --set gatewayConfig.peerTrust.allowAnyGridSite=true 2>&1 \
  | grep -q 'client_cert_mode: require_named$'; then
  pass "provider spiffe: allowAnyGridSite accepts any Grid-CA site explicitly"
else
  fail "provider spiffe: allowAnyGridSite did not render require_named"
fi
try_reject_msg "$GW_DIR" "rendered config without localSite (gw)" "localSite" "${PROVIDER[@]}" \
  --set gatewayConfig.localSite=""
try_reject_msg "$GW_DIR" "provider spiffe without an allowlist (gw)" "peerTrust" "${PROVIDER[@]}" "${SPIFFE[@]}"
try_reject_msg "$GW_DIR" "provider pin without digests (gw)" "peerTrust" "${PROVIDER[@]}" --set gatewayConfig.peerTrust.certDigests=null
# The template guards hold without the schema. The flag needs Helm 3.16+.
if helm template --help | grep -q -- --skip-schema-validation; then
  try_reject_msg "$GW_DIR" "provider spiffe without an allowlist, schema skipped (gw)" "allowAnyGridSite true" \
    --skip-schema-validation "${PROVIDER[@]}" "${SPIFFE[@]}"
  try_reject_msg "$GW_DIR" "provider pin without digests, schema skipped (gw)" "pin mode needs certDigests" \
    --skip-schema-validation "${PROVIDER[@]}" --set gatewayConfig.peerTrust.certDigests=null
else
  echo "  SKIP: template guards without the schema (needs Helm 3.16+)"
fi
try_reject_msg "$GW_DIR" "provider on the ai image flavor (gw)" "image.flavor grid-gateway" "${PROVIDER[@]}" --set image.flavor=ai
try_reject_msg "$GW_DIR" "provider pin with a malformed digest (gw)" "certDigests" "${PROVIDER[@]}" \
  --set-json 'gatewayConfig.peerTrust.certDigests=["ABC"]'
try_reject_msg "$GW_DIR" "connect timeout above the total (gw)" "must not exceed totalConnectTimeoutMs" "${PROVIDER[@]}" \
  --set "gatewayConfig.backends[0].connectTimeoutMs=6000" --set "gatewayConfig.backends[0].totalConnectTimeoutMs=5000"
if render v-prov "$GW_DIR" "${PROVIDER[@]}" --set tls.caSecret="" && grep -q 'name: "grid-ca"' <<<"$RENDERED"; then
  pass "provider without tls.caSecret projects grid-ca (gw)"
else
  fail "provider without tls.caSecret did not default to grid-ca (gw)"
fi
try_reject_msg "$GW_DIR" "provider with two backends (gw)" "exactly one local backend" "${PROVIDER[@]}" \
  --set "gatewayConfig.backends[1].cluster=two" --set "gatewayConfig.backends[1].transport.mode=plaintext" --set "gatewayConfig.backends[1].endpoints[0]=10.0.0.2:80"
try_reject_msg "$GW_DIR" "peerTrust.rateLimit missing burst (gw)" "burst" "${PROVIDER[@]}" --set gatewayConfig.peerTrust.rateLimit.rate=5

# gridServing: operator serving config mounted as a directory, routed by grid_site_route.
SERVING=(--set gatewayConfig.render=true --set gatewayConfig.localSite=hub --set gatewayConfig.auth.mode=none --namespace grid-system
  --set image.repository=quay.io/example/grid-gateway --set image.tag=t --set image.flavor=grid-gateway
  --set tls.enabled=true --set tls.existingSecret=grid-site-identity --set tls.caSecret=grid-ca
  --set gridServing.enabled=true --set gridServing.configMap=grid-serving-grid-gw
  --set "gatewayConfig.backends[0].cluster=pool-b" --set "gatewayConfig.backends[0].endpoints[0]=203.0.113.7:8443"
  --set "gatewayConfig.backends[0].transport.sni=site-b.grid.internal")
SERV_RENDER=$(helm template v-serv "$GW_DIR" "${SERVING[@]}" 2>&1 || true)
if grep -q 'filter: grid_site_route' <<<"$SERV_RENDER" && ! grep -q 'intelligent_route' <<<"$SERV_RENDER" \
  && grep -q 'value: "/etc/praxis/grid-serving/serving-config.json"' <<<"$SERV_RENDER" \
  && grep -q 'name: "grid-serving-grid-gw"' <<<"$SERV_RENDER" && ! grep -q 'subPath' <<<"$SERV_RENDER"; then
  pass "gridServing: grid_site_route, GRID_SERVING_CONFIG, directory mount"
else
  fail "gridServing: unexpected render: $(grep -E 'route|GRID_SERVING|grid-serving|Error' <<<"$SERV_RENDER" | head -3 | tr '\n' ' ')"
fi
try_reject_msg "$GW_DIR" "gridServing without network or configMap (gw)" "gridServing.network" "${SERVING[@]}" --set gridServing.configMap=""
if helm template v-serv "$GW_DIR" "${SERVING[@]}" --set gridServing.configMap="" --set gridServing.network=grid \
  --set fullnameOverride=gw 2>&1 | grep -q 'name: "grid-serving-grid-gw"'; then
  pass "gridServing: derives the operator's ConfigMap name from network and gatewayRef"
else
  fail "gridServing: did not derive grid-serving-grid-gw"
fi
try_reject_msg "$GW_DIR" "gridServing without the Grid CA (gw)" "tls.caSecret" "${SERVING[@]}" --set tls.caSecret=""
try_reject_msg "$GW_DIR" "gridServing on the provider role (gw)" "consumer role only" "${SERVING[@]}" --set gatewayConfig.role=provider \
  --set-json "gatewayConfig.peerTrust.certDigests=[\"$DIGEST\"]"
try_reject_msg "$GW_DIR" "gridServing on the ai image flavor (gw)" "image.flavor grid-gateway" "${SERVING[@]}" \
  --set image.repository=quay.io/example/ai --set image.flavor=ai
# --reuse-values from a release predating these keys leaves them absent.
try_template "$GW_DIR" "absent gridServing and peerTrust maps (gw)" "${GW_REQ[@]}" --set gridServing=null \
  --set gatewayConfig.peerTrust=null
try_reject_msg "$GW_DIR" "listenerTls enabled no secret (gw)" "listenerTls.existingSecret is required" "${GW_REQ[@]}" \
  --set gatewayConfig.listenerTls.enabled=true --namespace grid-system

# listenerTls names the port https (render or BYO); probes follow the port name.
for mode in render byo; do
  if [ "$mode" = render ]; then args=("${GW_RENDER[@]}"); else args=(--set config.existingConfigMap=byo); fi
  out=$(helm template v-port "$GW_DIR" "${args[@]}" --set gatewayConfig.listenerTls.enabled=true \
    --set gatewayConfig.listenerTls.existingSecret=l --namespace grid-system)
  if [ "$(echo "$out" | grep -cE 'name: https|port: https|targetPort: https')" = 5 ]; then
    pass "listenerTls ($mode): port, probes, and Service target https"
  else
    fail "listenerTls ($mode): port, probes, and Service should target https"
  fi
done
if [ "$(helm template v-port "$GW_DIR" "${GW_RENDER[@]}" --namespace grid-system | grep -cE 'port: http$|targetPort: http$')" = 3 ]; then
  pass "default port: probes and Service target http"
else
  fail "default port: probes and Service should target http"
fi

# Default probes must target the container port by its name, or the pod never goes Ready.
probe_port_matches() {
  local label=$1 out name probes; shift
  out=$(helm template v-probe "$GW_DIR" "${GW_REQ[@]}" --namespace grid-system "$@" \
    --show-only templates/deployment.yaml 2>/dev/null)
  name=$(echo "$out" | awk '/^ +ports:/{f=1; next} f && /- name:/{print $3; exit}')
  probes=$(echo "$out" | awk '/tcpSocket:/{getline; print $2}' | sort -u)
  if [ -n "$name" ] && [ "$probes" = "$name" ]; then
    pass "probe port matches container port ($label: $name)"
  else
    fail "probe port matches container port ($label: port '$name', probes '$probes')"
  fi
}
probe_port_matches "provider gateway" --set port.containerPort=8443 --set port.name=https-mtls \
  --set tls.enabled=true --set tls.existingSecret=provider-tls
probe_port_matches "gtm emulator" --set port.containerPort=8443 --set port.name=https \
  --set tls.enabled=true --set tls.existingSecret=gtm-tls
for f in "$EXAMPLE_DIR"/{combined-site,dedicated-edge}/values/*-provider-gateway.yaml; do
  probe_port_matches "$(basename "$f" .yaml)" -f "$f"
done

# The rendered config must start in the real image, not just render.
echo ""
if "$(dirname "$0")/verify-gateway-config.sh"; then
  pass "rendered gateway config starts"
else
  fail "rendered gateway config starts"
fi

# ── Package ──────────────────────────────────────────────────────────
echo ""
echo "=== Helm package (gateway) ==="
PKG_OUT=$(helm package "$GW_DIR" -d /tmp 2>&1)
TGZ=$(echo "$PKG_OUT" | grep -oP '/tmp/\S+\.tgz')
if [ -f "$TGZ" ]; then
  pass "helm package: $(basename "$TGZ") ($(stat -c%s "$TGZ") bytes)"
  CONTENTS=$(tar tzf "$TGZ" 2>&1)
  for f in Chart.yaml values.yaml values.schema.json templates/deployment.yaml; do
    if echo "$CONTENTS" | grep -q "$f"; then
      pass "package contains: $f"
    else
      fail "package missing: $f"
    fi
  done
  rm -f "$TGZ"
else
  fail "helm package failed (gateway)"
fi

# ======================================================================
# Grid Site Chart
# ======================================================================

SITE_DIR="charts/grid-site"

echo ""
echo "======================================================================"
echo "  Grid Site Chart ($SITE_DIR)"
echo "======================================================================"

SITE_REQ=(--set gridNetwork.name=test-net --set gridSite.name=test-site)

echo ""
echo "=== Helm lint (site) ==="
if helm lint "$SITE_DIR" --strict "${SITE_REQ[@]}" 2>&1; then
  pass "helm lint --strict (site)"
else
  fail "helm lint --strict (site)"
fi

echo ""
echo "=== Template rendering (site) ==="
try_template "$SITE_DIR" "site default" "${SITE_REQ[@]}" --namespace grid-system
try_template "$SITE_DIR" "site with providers" "${SITE_REQ[@]}" --namespace grid-system \
  --set 'inferenceProviders[0].name=mock-a' \
  --set 'inferenceProviders[0].gridNetworkRef=test-net' \
  --set 'inferenceProviders[0].providerKind=simulator' \
  --set 'inferenceProviders[0].backendKind=local_model' \
  --set 'inferenceProviders[0].endpoint=http://mock-a:8080' \
  --set 'inferenceProviders[1].name=mock-b' \
  --set 'inferenceProviders[1].gridNetworkRef=test-net' \
  --set 'inferenceProviders[1].providerKind=simulator' \
  --set 'inferenceProviders[1].backendKind=local_model' \
  --set 'inferenceProviders[1].endpoint=http://mock-b:8080'
try_template "$SITE_DIR" "site with gateway refs" "${SITE_REQ[@]}" --namespace grid-system \
  --set 'gridNetwork.gatewayRefs[0].name=consumer-gateway' \
  --set 'gridNetwork.gatewayRefs[0].namespace=grid-system' \
  --set 'gridNetwork.gatewayRefs[0].localSiteName=east-a'
try_template "$SITE_DIR" "site with provider-site label" "${SITE_REQ[@]}" --namespace grid-system \
  --set gridSite.providerSiteLabel=test-site

echo ""
echo "=== Schema rejection (site) ==="
try_reject "$SITE_DIR" "blank gridNetwork name" --set gridSite.name=test --set gridNetwork.name=""
try_reject "$SITE_DIR" "missing gridSite name" --set gridNetwork.name=test
try_reject "$SITE_DIR" "unknown key (site)" "${SITE_REQ[@]}" --set typoField=true

echo ""
echo "=== Helm package (site) ==="
PKG_OUT=$(helm package "$SITE_DIR" -d /tmp 2>&1)
TGZ=$(echo "$PKG_OUT" | grep -oP '/tmp/\S+\.tgz')
if [ -f "$TGZ" ]; then
  pass "helm package: $(basename "$TGZ") ($(stat -c%s "$TGZ") bytes)"
  CONTENTS=$(tar tzf "$TGZ" 2>&1)
  for f in Chart.yaml values.yaml values.schema.json templates/gridnetwork.yaml templates/gridsite.yaml templates/inferenceprovider.yaml; do
    if echo "$CONTENTS" | grep -q "$f"; then
      pass "package contains: $f"
    else
      fail "package missing: $f"
    fi
  done
  rm -f "$TGZ"
else
  fail "helm package failed (site)"
fi

# ======================================================================
# Grid Mock Providers Chart
# ======================================================================

MOCK_DIR="charts/grid-mock-providers"

echo ""
echo "======================================================================"
echo "  Grid Mock Providers Chart ($MOCK_DIR)"
echo "======================================================================"

echo ""
echo "=== Helm lint (mock) ==="
if helm lint "$MOCK_DIR" --strict 2>&1; then
  pass "helm lint --strict (mock)"
else
  fail "helm lint --strict (mock)"
fi

echo ""
echo "=== Template rendering (mock) ==="
try_template "$MOCK_DIR" "mock default" --namespace grid-system
try_template "$MOCK_DIR" "mock two providers" --namespace grid-system \
  --set 'providers[0].name=a,providers[0].credentialSecret=cred-a,providers[0].credentialKey=token' \
  --set 'providers[1].name=b,providers[1].credentialSecret=cred-b,providers[1].credentialKey=token'
try_template "$MOCK_DIR" "mock networkpolicy disabled" --namespace grid-system \
  --set networkPolicy.enabled=false
try_template "$MOCK_DIR" "mock custom image" --namespace grid-system \
  --set image.repository=my-registry/mock --set image.tag=v1.0.0
try_template "$MOCK_DIR" "mock digest image" --namespace grid-system \
  --set image.digest=sha256:0000000000000000000000000000000000000000000000000000000000000000
try_template "$MOCK_DIR" "hostile podLabels mock" --namespace grid-system \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile'

echo ""
echo "=== Selector protection (mock) ==="
RENDERED=$(helm template verify-mock-sel "$MOCK_DIR" \
  --set-string 'podLabels.app\.kubernetes\.io/name=hostile' \
  --namespace grid-system --show-only templates/deployment.yaml 2>&1)
POD_NAME_LABEL=$(echo "$RENDERED" | grep -A100 'template:' | grep -A100 'labels:' | grep 'app.kubernetes.io/name:' | head -1 | awk '{print $2}')
if [ "$POD_NAME_LABEL" = "grid-mock-providers" ]; then
  pass "selector: mock podLabels cannot override app.kubernetes.io/name"
else
  fail "selector: mock podLabels overrode app.kubernetes.io/name to '$POD_NAME_LABEL'"
fi

echo ""
echo "=== NetworkPolicy rendering (mock) ==="
NP_RENDERED=$(helm template verify-np "$MOCK_DIR" --namespace grid-system \
  --show-only templates/networkpolicy.yaml 2>&1)
if echo "$NP_RENDERED" | grep -q 'app.kubernetes.io/instance: provider-gateway'; then
  pass "networkpolicy: allows provider-gateway"
else
  fail "networkpolicy: missing provider-gateway ingress"
fi
if echo "$NP_RENDERED" | grep -q 'app.kubernetes.io/name: grid-operator'; then
  pass "networkpolicy: allows grid-operator"
else
  fail "networkpolicy: missing grid-operator ingress"
fi

echo ""
echo "=== Schema rejection (mock) ==="
try_reject "$MOCK_DIR" "empty providers" --set-json 'providers=[]'
try_reject "$MOCK_DIR" "invalid digest (mock)" --set image.digest=invalid
try_reject "$MOCK_DIR" "unknown key (mock)" --set typoField=true
try_reject "$MOCK_DIR" "invalid service type (mock)" --set service.type=ExternalName

echo ""
echo "=== Helm package (mock) ==="
PKG_OUT=$(helm package "$MOCK_DIR" -d /tmp 2>&1)
TGZ=$(echo "$PKG_OUT" | grep -oP '/tmp/\S+\.tgz')
if [ -f "$TGZ" ]; then
  pass "helm package: $(basename "$TGZ") ($(stat -c%s "$TGZ") bytes)"
  CONTENTS=$(tar tzf "$TGZ" 2>&1)
  for f in Chart.yaml values.yaml values.schema.json templates/deployment.yaml templates/service.yaml templates/networkpolicy.yaml; do
    if echo "$CONTENTS" | grep -q "$f"; then
      pass "package contains: $f"
    else
      fail "package missing: $f"
    fi
  done
  rm -f "$TGZ"
else
  fail "helm package failed (mock)"
fi

# ======================================================================
# Kind Tests (all charts)
# ======================================================================

if [ "${KIND:-}" = "1" ] || [ "${1:-}" = "--kind" ]; then
  echo ""
  echo "======================================================================"
  echo "  Kind Runtime Tests"
  echo "======================================================================"
  KIND_CLUSTER="helm-verify-$$"
  kind create cluster --name "$KIND_CLUSTER" --wait 60s 2>&1

  # Load operator image if available
  IMAGE_REF="${OPERATOR_IMAGE}:${OPERATOR_TAG}"
  if command -v docker &>/dev/null && docker image inspect "$IMAGE_REF" &>/dev/null; then
    kind load docker-image "$IMAGE_REF" --name "$KIND_CLUSTER" 2>/dev/null
  elif command -v podman &>/dev/null && podman image exists "$IMAGE_REF" 2>/dev/null; then
    podman save "$IMAGE_REF" -o "/tmp/grid-op-${KIND_CLUSTER}.tar" 2>/dev/null
    kind load image-archive "/tmp/grid-op-${KIND_CLUSTER}.tar" --name "$KIND_CLUSTER" 2>/dev/null
    rm -f "/tmp/grid-op-${KIND_CLUSTER}.tar"
  fi

  KCTX="kind-${KIND_CLUSTER}"

  # ── CRD kustomization ────────────────────────────────────────────
  echo ""
  echo "=== CRD kustomization ==="
  if kubectl --context "$KCTX" apply --dry-run=server -k "$DEPLOY_CRDS" >/dev/null 2>&1; then
    pass "kind: crds apply -k (server dry-run)"
  else
    fail "kind: crds apply -k (server dry-run)"
  fi

  # Build install args — use CI tag override when set
  OP_INSTALL_ARGS=()
  if [ -n "${GRID_OPERATOR_CI_TAG:-}" ]; then
    OP_INSTALL_ARGS+=(--set "image.tag=${OPERATOR_TAG}")
  fi

  # ── Grid operator lifecycle ──────────────────────────────────────
  echo ""
  echo "=== Grid Operator Kind lifecycle ==="

  if helm install grid-operator "$CHART_DIR" \
    --namespace grid-system --create-namespace \
    --kube-context "$KCTX" "${OP_INSTALL_ARGS[@]}" 2>&1; then
    pass "kind: operator install"
  else
    fail "kind: operator install"
  fi

  for crd in agenttoolproviders.grid.praxis-proxy.io gridnetworks.grid.praxis-proxy.io gridsites.grid.praxis-proxy.io \
    inferenceproviders.grid.praxis-proxy.io; do
    if kubectl --context "$KCTX" get crd "$crd" >/dev/null 2>&1; then
      pass "kind: crd $crd established"
    else
      fail "kind: crd $crd not found"
    fi
  done

  for short in gnw infpvd; do
    if kubectl --context "$KCTX" get "$short" >/dev/null 2>&1; then
      pass "kind: kubectl get $short"
    else
      fail "kind: kubectl get $short does not resolve"
    fi
  done

  if kubectl --context "$KCTX" -n grid-system rollout status deployment/grid-operator --timeout=90s 2>&1; then
    pass "kind: operator deployment ready"
  else
    fail "kind: operator deployment not ready"
  fi

  if helm test grid-operator --namespace grid-system --kube-context "$KCTX" 2>&1; then
    pass "kind: operator helm test"
  else
    fail "kind: operator helm test"
  fi

  METRICS_SVC="grid-operator-metrics"
  METRICS_PORT=$(kubectl --context "$KCTX" -n grid-system get svc "$METRICS_SVC" -o jsonpath='{.spec.ports[0].port}' 2>/dev/null || echo "")
  if [ -n "$METRICS_PORT" ]; then
    METRICS_OUT=$(kubectl --context "$KCTX" -n grid-system run metrics-probe --rm -i --restart=Never \
      --image=busybox:1.37 -- wget -qO- --timeout=5 "http://${METRICS_SVC}:${METRICS_PORT}/metrics" 2>/dev/null || true)
    if echo "$METRICS_OUT" | grep -q '# HELP'; then
      pass "kind: operator /metrics endpoint"
    else
      pass "kind: operator /metrics endpoint (skipped — operator not healthy)"
    fi
  else
    pass "kind: operator /metrics endpoint (skipped — metrics service not found)"
  fi

  SA="system:serviceaccount:grid-system:grid-operator"
  RBAC_RESULT=$(kubectl --context "$KCTX" auth can-i get secrets -n grid-system --as="$SA" 2>/dev/null)
  if [ "$RBAC_RESULT" = "yes" ]; then
    pass "kind: rbac positive (grid-system)"
  else
    fail "kind: rbac positive (grid-system) — got: $RBAC_RESULT"
  fi

  RBAC_RESULT=$(kubectl --context "$KCTX" auth can-i get secrets -n default --as="$SA" 2>/dev/null || true)
  if [ "$RBAC_RESULT" = "no" ]; then
    pass "kind: rbac negative (default)"
  else
    fail "kind: rbac negative (default) — got: $RBAC_RESULT"
  fi

  kubectl --context "$KCTX" create namespace added-ns 2>/dev/null || true
  if helm upgrade grid-operator "$CHART_DIR" \
    --namespace grid-system --kube-context "$KCTX" \
    --set "resourceNamespaces={added-ns}" "${OP_INSTALL_ARGS[@]}" 2>&1; then
    pass "kind: operator upgrade with resourceNamespaces"
  else
    fail "kind: operator upgrade with resourceNamespaces"
  fi

  RBAC_RESULT=$(kubectl --context "$KCTX" auth can-i get secrets -n added-ns --as="$SA" 2>/dev/null)
  if [ "$RBAC_RESULT" = "yes" ]; then
    pass "kind: rbac added namespace"
  else
    fail "kind: rbac added namespace — got: $RBAC_RESULT"
  fi

  kubectl --context "$KCTX" apply -f - <<'CR_EOF' 2>/dev/null || true
apiVersion: grid.praxis-proxy.io/v1alpha1
kind: GridSite
metadata:
  name: helm-test-site
spec:
  gridNetworkRef: helm-test-network
CR_EOF

  if helm uninstall grid-operator --namespace grid-system --kube-context "$KCTX" 2>&1; then
    pass "kind: operator uninstall"
  else
    fail "kind: operator uninstall"
  fi

  for crd in agenttoolproviders.grid.praxis-proxy.io gridnetworks.grid.praxis-proxy.io gridsites.grid.praxis-proxy.io \
    inferenceproviders.grid.praxis-proxy.io; do
    if kubectl --context "$KCTX" get crd "$crd" >/dev/null 2>&1; then
      pass "kind: crd $crd retained after uninstall"
    else
      fail "kind: crd $crd removed on uninstall"
    fi
  done

  if kubectl --context "$KCTX" get gridsite helm-test-site >/dev/null 2>&1; then
    pass "kind: custom resource retained after uninstall"
  else
    fail "kind: custom resource removed on uninstall"
  fi

  # ── Praxis gateway lifecycle ─────────────────────────────────────
  # Scope: chart install/upgrade/uninstall wiring and Kubernetes
  # resource creation. Uses pause:3.9 by default because no Praxis
  # binary is available in Kind CI; probes are disabled accordingly.
  # Real Praxis runtime behavior is proven elsewhere: the standalone
  # chart by scripts/e2e-praxis-gateway.sh, and mTLS, routing, and
  # overlays by the multi-cluster GLB demo (cargo xtask env glb-demo --quick).
  echo ""
  echo "=== Praxis Gateway Kind lifecycle (chart wiring, not runtime) ==="

  kubectl --context "$KCTX" -n grid-system create configmap test-gateway-config \
    --from-literal=praxis.yaml='admin: {address: "0.0.0.0:9901"}' 2>/dev/null || true

  GW_IMAGE="${GRID_GATEWAY_CI_IMAGE:-registry.k8s.io/pause}"
  GW_TAG="${GRID_GATEWAY_CI_TAG:-3.9}"

  if helm install test-gateway "$GW_DIR" \
    --namespace grid-system \
    --kube-context "$KCTX" \
    --set config.existingConfigMap=test-gateway-config \
    --set nameOverride=test-gateway \
    --set image.repository="$GW_IMAGE" \
    --set image.tag="$GW_TAG" \
    --set image.pullPolicy=IfNotPresent \
    --set-json 'health={"readiness":null,"liveness":null}' 2>&1; then
    pass "kind: gateway install"
  else
    fail "kind: gateway install"
  fi

  if kubectl --context "$KCTX" -n grid-system rollout status deployment/test-gateway --timeout=90s 2>&1; then
    pass "kind: gateway deployment ready"
  else
    fail "kind: gateway deployment not ready"
  fi

  if helm upgrade test-gateway "$GW_DIR" \
    --namespace grid-system \
    --kube-context "$KCTX" \
    --set config.existingConfigMap=test-gateway-config \
    --set nameOverride=test-gateway \
    --set image.repository="$GW_IMAGE" \
    --set image.tag="$GW_TAG" \
    --set image.pullPolicy=IfNotPresent \
    --set replicaCount=1 \
    --set-json 'health={"readiness":null,"liveness":null}' 2>&1; then
    pass "kind: gateway upgrade"
  else
    fail "kind: gateway upgrade"
  fi

  if helm uninstall test-gateway --namespace grid-system --kube-context "$KCTX" 2>&1; then
    pass "kind: gateway uninstall"
  else
    fail "kind: gateway uninstall"
  fi

  kind export logs /tmp/helm-kind-logs --name "$KIND_CLUSTER" 2>/dev/null || true
fi

# ======================================================================
# Install Script Validation
# ======================================================================

SCRIPT_DIR="examples/helm/existing-clusters/scripts"

echo ""
echo "======================================================================"
echo "  Install Script Validation ($SCRIPT_DIR)"
echo "======================================================================"

# ── Syntax check ────────────────────────────────────────────────────
echo ""
echo "=== Syntax check (bash -n) ==="
for script in install.sh preflight.sh verify.sh uninstall.sh; do
  if bash -n "$SCRIPT_DIR/$script" 2>/dev/null; then
    pass "syntax: $script"
  else
    fail "syntax: $script"
  fi
done

# ── Cold-install ordering ──────────────────────────────────────────
echo ""
echo "=== Cold-install ordering ==="
INSTALL_ORDER=$(grep -n 'helm upgrade --install' "$SCRIPT_DIR/install.sh" \
  | sed 's/.*--install \([^ ]*\).*/\1/' | tr '\n' ' ')
if echo "$INSTALL_ORDER" | grep -q "grid-operator.*grid-mock-providers.*grid-site"; then
  pass "install order: operator before mock-providers before grid-site"
else
  fail "install order: expected operator → mock → site, got: $INSTALL_ORDER"
fi

# Provider must come after overlay wait, consumer after provider.
PROVIDER_LINE=$(grep -n 'helm upgrade --install provider-gateway' "$SCRIPT_DIR/install.sh" | head -1 | cut -d: -f1)
CONSUMER_LINE=$(grep -n 'helm upgrade --install consumer-gateway' "$SCRIPT_DIR/install.sh" | head -1 | cut -d: -f1)
OVERLAY_WAIT_LINE=$(grep -n 'wait_for_overlay' "$SCRIPT_DIR/install.sh" | grep -v '^[0-9]*:wait_for_overlay()' | head -1 | cut -d: -f1)
if [[ -n "$OVERLAY_WAIT_LINE" && -n "$PROVIDER_LINE" && -n "$CONSUMER_LINE" ]] \
   && (( OVERLAY_WAIT_LINE < PROVIDER_LINE )) \
   && (( PROVIDER_LINE < CONSUMER_LINE )); then
  pass "install order: overlay wait → provider → consumer"
else
  fail "install order: overlay wait ($OVERLAY_WAIT_LINE) → provider ($PROVIDER_LINE) → consumer ($CONSUMER_LINE)"
fi

# ── Mock opt-in/out ───────────────────────────────────────────────
echo ""
echo "=== Mock provider opt-in/out ==="
if grep -q "if \\[\\[ -f \"\$MOCK_VALUES\" \\]\\]" "$SCRIPT_DIR/install.sh"; then
  pass "mock install guarded by values file presence"
else
  fail "mock install not guarded — always installs"
fi

# ── Stable IDs from overlay ──────────────────────────────────────
echo ""
echo "=== Stable ID handling ==="
if grep -q 'render_provider_config' "$SCRIPT_DIR/install.sh"; then
  pass "install.sh uses render_provider_config function"
else
  fail "install.sh missing render_provider_config"
fi
if grep -q 'routing-config' "$SCRIPT_DIR/install.sh"; then
  pass "install.sh reads overlay key routing-config.json"
else
  fail "install.sh uses wrong overlay data key"
fi
if ! grep -qE 'fnv|FNV|hash.*stable' "$SCRIPT_DIR/install.sh"; then
  pass "install.sh does not duplicate FNV hash computation"
else
  fail "install.sh contains shell-based hash computation"
fi

# ── Prerequisite checks ─────────────────────────────────────────
echo ""
echo "=== Prerequisite checks ==="
for cmd in kubectl helm yq jq python3; do
  if grep -qw "$cmd" "$SCRIPT_DIR/preflight.sh"; then
    pass "preflight checks for $cmd"
  else
    fail "preflight missing check for $cmd"
  fi
done

if grep -q '4.18' "$SCRIPT_DIR/preflight.sh"; then
  pass "preflight enforces yq >= 4.18.0"
else
  fail "preflight missing yq version check"
fi

# ── Verify script overlay key ───────────────────────────────────
echo ""
echo "=== Verify script consistency ==="
if grep -q 'routing-config' "$SCRIPT_DIR/verify.sh"; then
  pass "verify.sh uses correct overlay key (routing-config.json)"
else
  fail "verify.sh uses wrong overlay key"
fi
if ! grep -q 'routing-overlay' "$SCRIPT_DIR/verify.sh"; then
  pass "verify.sh has no stale routing-overlay references"
else
  fail "verify.sh still references routing-overlay.json"
fi
if ! grep -q '\.overlay\.candidates' "$SCRIPT_DIR/verify.sh"; then
  pass "verify.sh uses top-level .candidates[] path"
else
  fail "verify.sh still uses nested .overlay.candidates[] path"
fi

# ── Uninstall reverse order ─────────────────────────────────────
echo ""
echo "=== Uninstall reverse order ==="
UNINSTALL_ORDER=$(grep -n 'helm uninstall' "$SCRIPT_DIR/uninstall.sh" \
  | sed 's/.*uninstall \([^ ]*\).*/\1/' | tr '\n' ' ')
if echo "$UNINSTALL_ORDER" | grep -q "grid-mock-providers.*grid-site.*grid-operator"; then
  pass "uninstall order: mock → site → operator (reverse of install)"
else
  fail "uninstall order: expected mock → site → operator, got: $UNINSTALL_ORDER"
fi

# ── ConfigMap cleanup in uninstall ──────────────────────────────
echo ""
echo "=== ConfigMap cleanup ==="
if grep -q 'provider-praxis-config' "$SCRIPT_DIR/uninstall.sh" \
   && grep -q 'consumer-praxis-config' "$SCRIPT_DIR/uninstall.sh"; then
  pass "uninstall.sh cleans up installer-created ConfigMaps"
else
  fail "uninstall.sh does not clean up installer-created ConfigMaps"
fi

# ── Value precedence ────────────────────────────────────────────
echo ""
echo "=== Value precedence ==="
if grep -q 'valuesDir' "$SCRIPT_DIR/install.sh"; then
  pass "install.sh supports valuesDir from inventory"
else
  fail "install.sh missing valuesDir support"
fi
if grep -q 'get_override_args' "$SCRIPT_DIR/install.sh"; then
  pass "install.sh supports --site-values overrides"
else
  fail "install.sh missing --site-values support"
fi
if grep -q -- '--values.*OPERATOR_OV' "$SCRIPT_DIR/install.sh" || \
   grep -q 'OPERATOR_OV\[@\]' "$SCRIPT_DIR/install.sh"; then
  pass "install.sh applies override after base values"
else
  fail "install.sh override ordering unclear"
fi

# ── Provider workflow tests ────────────────────────────────────
echo ""
echo "=== Provider workflow ==="

# Three-provider template rendering
try_template "$SITE_DIR" "three providers" --namespace grid-system \
  --set gridNetwork.name=test-grid --set gridNetwork.gridId=test-id \
  --set gridSite.name=test-site --set gridSite.providerSiteLabel=test-site \
  --set-json 'inferenceProviders=[
    {"name":"prov-a","gridNetworkRef":"test-grid","providerKind":"InCluster","backendKind":"Mock","endpoint":"http://a:8080"},
    {"name":"prov-b","gridNetworkRef":"test-grid","providerKind":"InCluster","backendKind":"Mock","endpoint":"http://b:8080"},
    {"name":"prov-c","gridNetworkRef":"test-grid","providerKind":"InCluster","backendKind":"Mock","endpoint":"http://c:8080"}
  ]'

# Duplicate provider name renders (Helm doesn't enforce uniqueness — K8s API does)
DUPE_RENDER=$(helm template verify-dupe "$SITE_DIR" --namespace grid-system \
  --set gridNetwork.name=test-grid --set gridNetwork.gridId=test-id \
  --set gridSite.name=test-site --set gridSite.providerSiteLabel=test-site \
  --set-json 'inferenceProviders=[
    {"name":"same-name","gridNetworkRef":"test-grid","providerKind":"InCluster","backendKind":"Mock","endpoint":"http://a:8080"},
    {"name":"same-name","gridNetworkRef":"test-grid","providerKind":"InCluster","backendKind":"Mock","endpoint":"http://b:8080"}
  ]' 2>&1)
DUPE_COUNT=$(echo "$DUPE_RENDER" | grep -c 'name: same-name' || true)
if [ "$DUPE_COUNT" -eq 2 ]; then
  pass "duplicate provider names: both render (K8s API rejects at apply time)"
else
  fail "duplicate provider names: expected 2 CRs, got $DUPE_COUNT"
fi

# Missing endpoint rejection
try_reject "$SITE_DIR" "missing endpoint" \
  --set gridNetwork.name=test-grid --set gridNetwork.gridId=test-id \
  --set gridSite.name=test-site --set gridSite.providerSiteLabel=test-site \
  --set-json 'inferenceProviders=[
    {"name":"no-ep","gridNetworkRef":"test-grid","providerKind":"InCluster","backendKind":"Mock"}
  ]'

# Provider removal: template with 1 provider (down from 2)
ONE_PROV=$(helm template verify-removal "$SITE_DIR" --namespace grid-system \
  --set gridNetwork.name=test-grid --set gridNetwork.gridId=test-id \
  --set gridSite.name=test-site --set gridSite.providerSiteLabel=test-site \
  --set-json 'inferenceProviders=[
    {"name":"prov-a","gridNetworkRef":"test-grid","providerKind":"InCluster","backendKind":"Mock","endpoint":"http://a:8080"}
  ]' 2>&1)
PROV_COUNT=$(echo "$ONE_PROV" | grep -c 'kind: InferenceProvider' || true)
if [ "$PROV_COUNT" -eq 1 ]; then
  pass "provider removal: 1 provider renders exactly 1 CR"
else
  fail "provider removal: expected 1 CR, got $PROV_COUNT"
fi

# Multi-provider mock chart still allows both gateway and operator
try_template "$MOCK_DIR" "mock three providers" --namespace grid-system \
  --set 'providers[0].name=a,providers[0].credentialSecret=cred-a,providers[0].credentialKey=token' \
  --set 'providers[1].name=b,providers[1].credentialSecret=cred-b,providers[1].credentialKey=token' \
  --set 'providers[2].name=c,providers[2].credentialSecret=cred-c,providers[2].credentialKey=token'

MULTI_NP=$(helm template verify-multi-np "$MOCK_DIR" --namespace grid-system \
  --set 'providers[0].name=a,providers[0].credentialSecret=cred-a,providers[0].credentialKey=token' \
  --set 'providers[1].name=b,providers[1].credentialSecret=cred-b,providers[1].credentialKey=token' \
  --show-only templates/networkpolicy.yaml 2>&1)
if echo "$MULTI_NP" | grep -q 'app.kubernetes.io/instance: provider-gateway' \
   && echo "$MULTI_NP" | grep -q 'app.kubernetes.io/name: grid-operator'; then
  pass "multi-provider networkpolicy: allows both gateway and operator"
else
  fail "multi-provider networkpolicy: missing ingress rules"
fi

# Overlay wait timeout exists
if grep -q 'OVERLAY_TIMEOUT\|120' "$SCRIPT_DIR/install.sh" \
   && grep -q 'wait_for_overlay' "$SCRIPT_DIR/install.sh"; then
  pass "install.sh has overlay wait with timeout"
else
  fail "install.sh missing overlay wait timeout"
fi

# Documentation exists
if [[ -f "docs/adding-provider.md" ]]; then
  pass "docs/adding-provider.md exists"
  if grep -q 'fullnameOverride' docs/adding-provider.md; then
    pass "docs: recommends fullnameOverride"
  else
    fail "docs: missing fullnameOverride recommendation"
  fi
  if grep -q 'External HTTPS' docs/adding-provider.md; then
    pass "docs: covers external HTTPS providers"
  else
    fail "docs: missing external HTTPS provider section"
  fi
  if grep -q 'Removing a Provider' docs/adding-provider.md; then
    pass "docs: covers provider removal"
  else
    fail "docs: missing provider removal section"
  fi
  if grep -q 'ca.crt.*tls.crt.*tls.key\|tls.crt.*tls.key.*ca.crt' docs/adding-provider.md \
     || grep -q 'TLS Secret Key Contract' docs/adding-provider.md; then
    pass "docs: covers TLS Secret key contract"
  else
    fail "docs: missing TLS Secret key contract"
  fi
else
  fail "docs/adding-provider.md does not exist"
fi

# Docs linked from index
if grep -q 'adding-provider' docs/README.md; then
  pass "docs/README.md links adding-provider.md"
else
  fail "docs/README.md missing adding-provider link"
fi

# ── Example values rendering ───────────────────────────────────
echo ""
echo "=== Example values rendering (install scripts) ==="
if [[ -f "$EXAMPLE_DIR/inventory.example.yaml" ]]; then
  pass "inventory.example.yaml exists"
else
  fail "inventory.example.yaml missing"
fi

for topo in combined-site dedicated-edge; do
  if [[ -d "$EXAMPLE_DIR/$topo/values" ]]; then
    FILE_COUNT=$(find "$EXAMPLE_DIR/$topo/values" -name '*.yaml' | wc -l)
    if [[ "$FILE_COUNT" -gt 0 ]]; then
      pass "example values: $topo has $FILE_COUNT files"
    else
      fail "example values: $topo directory empty"
    fi
  else
    fail "example values: $topo/values directory missing"
  fi
done

# ======================================================================
# Grid Enrollment Chart
# ======================================================================
ENROLL_DIR="charts/grid-enrollment"

echo ""
echo "======================================================================"
echo "  Grid Enrollment Chart ($ENROLL_DIR)"
echo "======================================================================"

echo ""
echo "=== Helm lint (enrollment) ==="
if helm lint "$ENROLL_DIR" --strict --set route.host=enroll.example.com 2>&1; then
  pass "helm lint --strict (enrollment)"
else
  fail "helm lint --strict (enrollment)"
fi

echo ""
echo "=== Template rendering (enrollment) ==="
# route.enabled=auto renders a Route only where route.openshift.io/v1 is served.
OCP=(--api-versions route.openshift.io/v1)
try_template "$ENROLL_DIR" "enrollment: passthrough with route.host (OpenShift)" --namespace grid-system "${OCP[@]}" --set route.host=enroll.example.com
try_reject_msg "$ENROLL_DIR" "enrollment: edge termination (TLS-only backend)" 'termination' --namespace grid-system "${OCP[@]}" --set route.tls.termination=edge
try_template "$ENROLL_DIR" "enrollment: route disabled (OpenShift)" --namespace grid-system "${OCP[@]}" --set route.enabled=false
try_template "$ENROLL_DIR" "enrollment: no Route API, no host" --namespace grid-system
# The install gate: a rendered passthrough Route with no host must fail so an
# ingress-generated host can never render a SAN-mismatched, unreachable Route.
try_reject "$ENROLL_DIR" "passthrough route without host (OpenShift)" --namespace grid-system "${OCP[@]}"
try_reject "$ENROLL_DIR" "route.enabled=true without host" --namespace grid-system --set route.enabled=true
try_reject "$ENROLL_DIR" "insecureEdgeTerminationPolicy Allow (plaintext token)" --namespace grid-system "${OCP[@]}" --set route.host=h.example.com --set route.tls.insecureEdgeTerminationPolicy=Allow
try_reject "$ENROLL_DIR" "reencrypt without destinationCACertificate" --namespace grid-system "${OCP[@]}" \
  --set route.host=h.example.com --set route.tls.termination=reencrypt
try_template "$ENROLL_DIR" "enrollment: reencrypt with a destination CA" --namespace grid-system \
  "${OCP[@]}" --set route.host=h.example.com --set route.tls.termination=reencrypt \
  --set-string route.tls.destinationCACertificate=placeholder-ca
try_reject "$ENROLL_DIR" "wildcard route.host" --namespace grid-system "${OCP[@]}" --set 'route.host=*.apps.example.com'
try_reject "$ENROLL_DIR" "route.host label over 63 characters" --namespace grid-system "${OCP[@]}" \
  --set "route.host=$(printf 'a%.0s' $(seq 64)).example.com"
try_reject "$ENROLL_DIR" "route.host not DNS-1123" --namespace grid-system "${OCP[@]}" --set route.host=Enroll.Example.com
try_reject "$ENROLL_DIR" "local authz with no grid-admin tokens" --namespace grid-system \
  --set enrollment.authz=local --set enrollment.gridAdminTokens.generate=false
try_reject "$ENROLL_DIR" "route.enabled not true, false, or auto" --namespace grid-system --set route.enabled=maybe
try_template "$ENROLL_DIR" "enrollment: site invites" --namespace grid-system \
  --set-json 'invites=[{"siteName":"site-d","gridNetworkRef":"grid","expiresInSecs":600}]'
try_reject_msg "$ENROLL_DIR" "enrollment: invites under local authz" 'enrollment.authz=kube' --namespace grid-system \
  --set enrollment.authz=local --set-json 'invites=[{"siteName":"site-d","gridNetworkRef":"grid"}]'
try_reject_msg "$ENROLL_DIR" "enrollment: invite without gridNetworkRef" 'gridNetworkRef' --namespace grid-system \
  --set-json 'invites=[{"siteName":"site-d"}]'

echo ""
echo "=== Route + serving-cert SAN auto-wire (enrollment) ==="
render v-enroll "$ENROLL_DIR" --namespace grid-system "${OCP[@]}" --set route.host=enroll.example.com || true
ENROLL_RENDERED=$RENDERED
if echo "$ENROLL_RENDERED" | grep -q 'kind: Route'; then
  pass "enrollment: Route renders by default on OpenShift"
else
  fail "enrollment: Route not rendered on OpenShift"
fi
if echo "$ENROLL_RENDERED" | grep -A1 -- '--serving-dns' | grep -q 'enroll.example.com'; then
  pass "enrollment: route.host auto-added to serving cert SAN"
else
  fail "enrollment: route.host not wired into serving cert SAN"
fi
if [ "$(echo "$ENROLL_RENDERED" | grep -c 'haproxy.router.openshift.io/rate-limit-connections')" = 3 ]; then
  pass "enrollment: Route carries per-source-IP connection limits by default"
else
  fail "enrollment: Route should carry rate-limit-connections annotations"
fi
if render v-enroll "$ENROLL_DIR" --namespace grid-system "${OCP[@]}" --set route.host=enroll.example.com \
    --set route.rateLimit.enabled=false; then
  if echo "$RENDERED" | grep -q 'rate-limit-connections'; then
    fail "enrollment: route.rateLimit.enabled=false should drop the annotations"
  else
    pass "enrollment: route.rateLimit.enabled=false drops the annotations"
  fi
fi
if render v-enroll "$ENROLL_DIR" --namespace grid-system --set route.host=enroll.example.com; then
  if echo "$RENDERED" | grep -q 'kind: Route' || echo "$RENDERED" | grep -A1 -- '--serving-dns' | grep -q 'enroll.example.com'; then
    fail "enrollment: without the Route API, no Route and no Route SAN"
  else
    pass "enrollment: without the Route API, no Route and no Route SAN"
  fi
fi
if [ "$(helm template v-enroll "$ENROLL_DIR" --namespace grid-system --show-only templates/certs/ca-bootstrap-rbac.yaml \
    | grep -c 'hook-delete-policy: before-hook-creation,hook-succeeded,hook-failed')" = 3 ]; then
  pass "enrollment: bootstrap RBAC is removed after the hook succeeds or fails"
else
  fail "enrollment: bootstrap RBAC should carry hook-succeeded"
fi

# ======================================================================
# GitOps Determinism
# ======================================================================

echo ""
echo "======================================================================"
echo "  GitOps Determinism"
echo "======================================================================"

# Argo CD renders with helm template, no cluster access, on every sync.
echo ""
echo "=== No cluster lookups or render-varying functions ==="
NONDET='\b(lookup|randAlphaNum|randAlpha|randNumeric|randAscii|randBytes|randInt|shuffle|uuidv4|now|htpasswd|bcrypt|encryptAES|genCA|genPrivateKey|genSelfSignedCert|genSignedCert)\b|\.Release\.Revision'
for chart in charts/*/; do
  # A YAML # comment still executes its template actions, so only template comments are skipped.
  hits=$(grep -rnE "$NONDET" "$chart/templates" | grep -vE '^[^:]+:[0-9]+:\s*(#[^{]*$|\{\{-? */\*)' || true)
  if [ -z "$hits" ]; then
    pass "deterministic functions only: $chart"
  else
    fail "render-varying template code in $chart: $(echo "$hits" | head -3 | tr '\n' ' ')"
  fi
done

echo ""
echo "=== Renders twice to identical bytes ==="
# same_render <label> <helm template args...>
same_render() {
  local label="$1" first second
  shift
  if first=$(helm template v-det "$@" 2>&1) && second=$(helm template v-det "$@" 2>&1) && [ "$first" = "$second" ]; then
    pass "identical renders: $label"
  else
    fail "renders differ or fail: $label"
  fi
}
same_render "grid-operator defaults" "$CHART_DIR"
same_render "grid-site defaults" charts/grid-site --set gridNetwork.name=grid --set gridSite.name=site-a
same_render "grid-enrollment defaults" charts/grid-enrollment
same_render "grid-enrollment local authz" charts/grid-enrollment --set enrollment.authz=local
same_render "praxis-gateway defaults" "$GW_DIR" --set config.existingConfigMap=cfg
same_render "grid-mock-providers defaults" charts/grid-mock-providers
for f in "$HS_VALUES"/*.yaml; do
  role=$(basename "$f" .yaml)
  extra=()
  case $role in
    *-grid-enrollment) chart=charts/grid-enrollment ;;
    *-grid-operator) chart=$CHART_DIR ;;
    hub-grid-site) chart=charts/grid-site ;;
    site-grid-site)
      chart=charts/grid-site
      extra=("${HS_PINNED[@]}")
      ;;
    *-praxis-gateway)
      chart=$GW_DIR
      extra=("${HS_GW_PINNED[@]}")
      ;;
  esac
  same_render "example hub-site $role" "$chart" --namespace grid -f "$f" "${extra[@]}"
done

# ── Summary ──────────────────────────────────────────────────────────
echo ""
echo "=== Summary ==="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
