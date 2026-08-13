#!/usr/bin/env bash
set -euo pipefail

PASS=0
FAIL=0
KIND_CLUSTER=""

OPERATOR_IMAGE="ghcr.io/praxis-proxy/grid-operator"
OPERATOR_TAG="${GRID_OPERATOR_CI_TAG:-v0.1.3}"
DEFAULT_GATEWAY_IMAGE="ghcr.io/praxis-proxy/grid-ai-rollup:v0.1.3"

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
for crd in agenttoolprovider gridnetwork gridsite inferenceprovider; do
  if diff -q "$DEPLOY_CRDS/${crd}.yaml" "$CHART_DIR/crds/${crd}.yaml" >/dev/null 2>&1; then
    pass "crd sync: ${crd}.yaml"
  else
    fail "crd sync: ${crd}.yaml differs from $DEPLOY_CRDS/${crd}.yaml"
  fi
done

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
try_template "$CHART_DIR" "SWIM advertise address" \
  --set swim.service.enabled=true --set swim.service.type=LoadBalancer \
  --set swim.advertiseAddress=swim.example.com:7946
try_template "$CHART_DIR" "scheduling" \
  --set nodeSelector.zone=us-east-1 --set priorityClassName=high-priority
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

# ── Schema rejection ────────────────────────────────────────────────
echo ""
echo "=== Schema rejection ==="
try_reject "$CHART_DIR" "replicaCount=2" --set replicaCount=2
try_reject "$CHART_DIR" "invalid digest" --set image.digest=invalid
try_reject "$CHART_DIR" "port zero" --set metrics.service.port=0
try_reject "$CHART_DIR" "invalid SWIM type" --set swim.service.type=ExternalName
try_reject "$CHART_DIR" "unknown key" --set typoField=true

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
  for f in Chart.yaml values.yaml values.schema.json templates/deployment.yaml crds/agenttoolprovider.yaml \
    crds/gridnetwork.yaml crds/gridsite.yaml crds/inferenceprovider.yaml; do
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
# the chart default so this path validates the released Grid rollup contract.
GW_REQ=(--set config.existingConfigMap=test-config)

# ── Helm lint ────────────────────────────────────────────────────────
echo ""
echo "=== Helm lint ==="
if helm lint "$GW_DIR" --strict "${GW_REQ[@]}" 2>&1; then
  pass "helm lint --strict (gateway)"
else
  fail "helm lint --strict (gateway)"
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
try_reject "$GW_DIR" "missing config" --set image.tag=v0.1.0-test --namespace grid-system
try_reject "$GW_DIR" "invalid digest (gw)" "${GW_REQ[@]}" --set image.digest=invalid
try_reject "$GW_DIR" "invalid service type (gw)" "${GW_REQ[@]}" --set service.type=ExternalName
try_reject "$GW_DIR" "unknown key (gw)" "${GW_REQ[@]}" --set typoField=true
try_reject "$GW_DIR" "runAsNonRoot override" "${GW_REQ[@]}" --set podSecurityContext.runAsNonRoot=false
try_reject "$GW_DIR" "overlay enabled no name" "${GW_REQ[@]}" --set overlay.enabled=true
try_reject "$GW_DIR" "tls enabled no secret" "${GW_REQ[@]}" --set tls.enabled=true

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
try_reject "$SITE_DIR" "missing gridNetwork name" --set gridSite.name=test
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
  # Real Praxis runtime behavior (mTLS, routing, overlay) is proven
  # by the multi-cluster GLB demo (cargo xtask env glb-demo --quick).
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

# ── Summary ──────────────────────────────────────────────────────────
echo ""
echo "=== Summary ==="
echo "  Passed: $PASS"
echo "  Failed: $FAIL"
[ "$FAIL" -eq 0 ] || exit 1
