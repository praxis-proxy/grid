#!/usr/bin/env bash
# Deploy postgres, MaaS/KServe CRDs, KServe/llmisvc, Gateway, and stock maas-controller.
# Controller reconciles maas-api + IPP (EnvoyFilters). Requires MAAS_ROOT.
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_maas_root
require_cmd kubectl
require_cmd python3
require_cmd openssl
if ! command -v kustomize >/dev/null 2>&1 && ! kubectl kustomize --help >/dev/null 2>&1; then
  die "missing required tool: kustomize (or kubectl with kustomize support)"
fi

ARCH=$(uname -m)
case "$ARCH" in
  arm64|aarch64) ARCH=arm64 ;;
  x86_64|amd64) ARCH=amd64 ;;
esac

# ── PostgreSQL ──────────────────────────────────────────────────────────────
kc create namespace "$MAAS_NAMESPACE" --dry-run=client -o yaml | kc apply -f -

if ! kc get deployment postgres -n "$MAAS_NAMESPACE" &>/dev/null; then
  POSTGRES_USER="maas"
  POSTGRES_DB="maas"
  POSTGRES_PASSWORD="$(openssl rand -base64 32 | tr -d '/+=' | cut -c1-32)"

  kc apply -n "$MAAS_NAMESPACE" -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: postgres-creds
  labels:
    app: postgres
stringData:
  POSTGRES_USER: "${POSTGRES_USER}"
  POSTGRES_PASSWORD: "${POSTGRES_PASSWORD}"
  POSTGRES_DB: "${POSTGRES_DB}"
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: postgres
  labels:
    app: postgres
spec:
  replicas: 1
  selector:
    matchLabels:
      app: postgres
  template:
    metadata:
      labels:
        app: postgres
    spec:
      containers:
      - name: postgres
        image: postgres:16-alpine
        env:
        - name: POSTGRES_USER
          valueFrom:
            secretKeyRef:
              name: postgres-creds
              key: POSTGRES_USER
        - name: POSTGRES_PASSWORD
          valueFrom:
            secretKeyRef:
              name: postgres-creds
              key: POSTGRES_PASSWORD
        - name: POSTGRES_DB
          valueFrom:
            secretKeyRef:
              name: postgres-creds
              key: POSTGRES_DB
        ports:
        - containerPort: 5432
        volumeMounts:
        - name: data
          mountPath: /var/lib/postgresql/data
        resources:
          requests:
            memory: "256Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "500m"
        readinessProbe:
          exec:
            command: ["pg_isready", "-U", "maas"]
          initialDelaySeconds: 5
          periodSeconds: 5
      volumes:
      - name: data
        emptyDir: {}
---
apiVersion: v1
kind: Service
metadata:
  name: postgres
  labels:
    app: postgres
spec:
  selector:
    app: postgres
  ports:
  - port: 5432
    targetPort: 5432
EOF

  ENCODED_PASSWORD=$(printf '%s' "$POSTGRES_PASSWORD" | od -An -tx1 | tr -d ' \n' | sed 's/../%&/g')
  DB_CONNECTION_URL="postgresql://${POSTGRES_USER}:${ENCODED_PASSWORD}@postgres:5432/${POSTGRES_DB}?sslmode=disable"
  printf '%s' "$DB_CONNECTION_URL" | \
    kubectl --context "$KUBE_CONTEXT" create secret generic maas-db-config \
      --from-file=DB_CONNECTION_URL=/dev/stdin \
      --dry-run=client -o yaml | \
    kubectl --context "$KUBE_CONTEXT" label --local -f - app=maas-api --dry-run=client -o yaml | \
    kc apply -n "$MAAS_NAMESPACE" -f -

  kc wait -n "$MAAS_NAMESPACE" --for=condition=available deployment/postgres --timeout=120s
  ok "PostgreSQL deployed"
else
  ok "PostgreSQL already deployed"
fi

# ── OpenShift stub CRDs ─────────────────────────────────────────────────────
for stub_crd in \
  "authentications.config.openshift.io:Authentication:AuthenticationList" \
  "routes.route.openshift.io:Route:RouteList"; do
  crd_name="${stub_crd%%:*}"
  rest="${stub_crd#*:}"
  kind_name="${rest%%:*}"
  list_name="${rest#*:}"
  group="${crd_name#*.}"
  if ! kc get crd "$crd_name" &>/dev/null; then
    kc apply -f - <<EOF
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: ${crd_name}
spec:
  group: ${group}
  names:
    kind: ${kind_name}
    listKind: ${list_name}
    plural: ${crd_name%%.*}
    singular: $(echo "${kind_name}" | tr '[:upper:]' '[:lower:]')
  scope: Namespaced
  versions:
  - name: v1
    served: true
    storage: true
    schema:
      openAPIV3Schema:
        type: object
        x-kubernetes-preserve-unknown-fields: true
EOF
  fi
done

# ── MaaS CRDs ───────────────────────────────────────────────────────────────
if ! kc get crd externalmodels.maas.opendatahub.io &>/dev/null; then
  kc apply -f "$MAAS_ROOT/deployment/base/maas-controller/crd/bases/"
  for crd in configs.maas.opendatahub.io maastenantconfigs.maas.opendatahub.io tenants.maas.opendatahub.io \
             externalmodels.maas.opendatahub.io maasmodelrefs.maas.opendatahub.io \
             maassubscriptions.maas.opendatahub.io maasauthpolicies.maas.opendatahub.io; do
    if kc get "crd/$crd" &>/dev/null; then
      wait_for_crd_established "$crd" 120
    fi
  done
  ok "MaaS CRDs installed"
else
  ok "MaaS CRDs already installed"
fi

# ── KServe LLMInferenceService CRDs + llmisvc controller ────────────────────
# CRDs and controller MUST come from the same KSERVE_COMMIT. Mixing an older
# v1alpha1-only CRD pin with a newer controller yields:
#   no matches for kind "LLMInferenceService" in version ".../v1alpha2"
ensure_kserve_clone

echo "  Applying LLMInferenceService CRDs from ${KSERVE_COMMIT}..."
# Use kustomize so conversion-webhook patches apply; never kubectl-apply the
# directory (it also contains kustomization.yaml / patch CRs).
kustomize_build "$KSERVE_CLONE/config/crd/full/llmisvc" | kc apply --server-side --force-conflicts -f -
wait_for_crd_established llminferenceservices.serving.kserve.io 120
wait_for_crd_established llminferenceserviceconfigs.serving.kserve.io 120 optional
ok "KServe LLMInferenceService CRDs installed (${KSERVE_COMMIT})"

# ── Vanilla KServe (main controller scaled to 0; llmisvc owns LLMIS) ────────
if [[ -x "$MAAS_ROOT/scripts/installers/install-kserve.sh" ]]; then
  if ! kc get deployment kserve-controller-manager -n kserve &>/dev/null; then
    # install-kserve uses current context
    kubectl config use-context "$KUBE_CONTEXT" >/dev/null
    "$MAAS_ROOT/scripts/installers/install-kserve.sh"
    kc scale deployment/kserve-controller-manager --replicas=0 -n kserve || true
    ok "KServe installed (main controller scaled to 0)"
  else
    ok "KServe already installed"
  fi
else
  warn "MAAS_ROOT missing scripts/installers/install-kserve.sh — skipping vanilla KServe install"
fi

if [[ "$ARCH" == "arm64" ]]; then
  require_cmd docker
  echo "  Building llmisvc-controller for arm64..."
  (cd "$KSERVE_CLONE" && docker buildx build --platform linux/arm64 --load \
    -f llmisvc-controller.Dockerfile \
    -t "$LLMISVC_IMAGE" .)
  kind load docker-image "$LLMISVC_IMAGE" --name "$KIND_CLUSTER_NAME"
fi

echo "  Applying llmisvc controller from ${KSERVE_COMMIT}..."
kustomize_build "$KSERVE_CLONE/config/llmisvc/" | kc apply --server-side --force-conflicts -f -
apply_llmisvc_distro_rbac
kc -n kserve set image deployment/llmisvc-controller-manager "manager=${LLMISVC_IMAGE}"
patch_llmisvc_resources
# cert-manager stack creates maas-root-ca before this stack runs.
kc wait --for=condition=Ready "certificate/maas-root-ca" -n "$MAAS_NAMESPACE" --timeout=120s
patch_llmisvc_signing_ca
kc -n kserve rollout status deployment/llmisvc-controller-manager --timeout=180s

_retries=0
while [[ $_retries -lt 12 ]]; do
  if kc apply -f "$KSERVE_CLONE/config/llmisvcconfig/config-llm-template.yaml" -n kserve 2>/dev/null; then
    break
  fi
  sleep 5
  _retries=$((_retries + 1))
done
for f in "$KSERVE_CLONE"/config/llmisvcconfig/config-*.yaml; do
  [[ -f "$f" ]] || continue
  kc apply -f "$f" -n kserve || true
done
# Webhook must answer before fixtures / any LLMInferenceService create.
kc create namespace llm-internal --dry-run=client -o yaml | kc apply -f -
wait_for_llmisvc_webhook llm-internal
ok "LLMIS controller installed (${LLMISVC_IMAGE})"

# ── Gateway ─────────────────────────────────────────────────────────────────
# maas-api ResolveGatewayInternalHost only accepts Gateway-owned Services that
# expose port 443. An HTTP-only listener leaves the Service on :80 and maas-api
# exits with "gateway service not found".
if ! kc get certificate maas-default-gateway-tls -n "$GATEWAY_NAMESPACE" &>/dev/null; then
  kc apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: maas-default-gateway-tls
  namespace: ${GATEWAY_NAMESPACE}
spec:
  secretName: maas-default-gateway-tls
  issuerRef:
    name: maas-selfsigned-issuer
    kind: ClusterIssuer
  dnsNames:
  - maas-default-gateway-istio.${GATEWAY_NAMESPACE}.svc
  - maas-default-gateway-istio.${GATEWAY_NAMESPACE}.svc.cluster.local
  - localhost
  duration: 8760h
  renewBefore: 720h
EOF
  kc wait --for=condition=Ready "certificate/maas-default-gateway-tls" \
    -n "$GATEWAY_NAMESPACE" --timeout=120s
fi

kc apply -f - <<EOF
apiVersion: gateway.networking.k8s.io/v1
kind: Gateway
metadata:
  name: maas-default-gateway
  namespace: ${GATEWAY_NAMESPACE}
  labels:
    app.kubernetes.io/name: maas
    app.kubernetes.io/instance: maas-default-gateway
spec:
  gatewayClassName: istio
  listeners:
  - name: http
    port: 80
    protocol: HTTP
    allowedRoutes:
      namespaces:
        from: All
  - name: https
    port: 443
    protocol: HTTPS
    allowedRoutes:
      namespaces:
        from: All
    tls:
      mode: Terminate
      certificateRefs:
      - name: maas-default-gateway-tls
EOF
kc wait --for=condition=Programmed "gateway/maas-default-gateway" \
  -n "$GATEWAY_NAMESPACE" --timeout=180s || \
  warn "Gateway not Programmed after 180s (continuing)"
ok "Gateway ready (HTTP:80 + HTTPS:443)"

kc delete destinationrule maas-api-no-mtls -n "$MAAS_NAMESPACE" 2>/dev/null || true

if ! kc get peerauthentication maas-permissive -n "$MAAS_NAMESPACE" &>/dev/null; then
  kc apply -f - <<EOF
apiVersion: security.istio.io/v1
kind: PeerAuthentication
metadata:
  name: maas-permissive
  namespace: ${MAAS_NAMESPACE}
spec:
  mtls:
    mode: PERMISSIVE
---
apiVersion: networking.k8s.io/v1
kind: NetworkPolicy
metadata:
  name: maas-gateway-allow
  namespace: ${MAAS_NAMESPACE}
spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/name: maas-api
  policyTypes:
  - Ingress
  ingress:
  - from:
    - namespaceSelector:
        matchLabels:
          kubernetes.io/metadata.name: ${GATEWAY_NAMESPACE}
EOF
  ok "Istio networking configured"
fi

# ── MaaS controller (owns IPP EnvoyFilters) ─────────────────────────────────
# Use the xKS overlay: Kind has no OpenShift service-CA, so the default
# overlay never creates secret/maas-controller-webhook-cert. xKS issues that
# secret via cert-manager and injects the webhook CA from it.
kc create namespace "$SUBSCRIPTION_NAMESPACE" --dry-run=client -o yaml | kc apply -f -

TEMP_DIR=$(mktemp -d)
trap 'rm -rf "$TEMP_DIR"' EXIT

CTRL_BASE="$TEMP_DIR/deployment/base/maas-controller"
mkdir -p "$CTRL_BASE/overlays"
for d in crd rbac manager webhook; do
  ln -s "$MAAS_ROOT/deployment/base/maas-controller/$d" "$CTRL_BASE/$d"
done
cp -a "$MAAS_ROOT/deployment/base/maas-controller/overlays/xks" "$CTRL_BASE/overlays/xks"

cat > "$CTRL_BASE/overlays/xks/params.env" <<EOF
maas-api-image=${MAAS_API_IMAGE}
maas-controller-image=${MAAS_CONTROLLER_IMAGE}
payload-processing-image=${IPP_IMAGE}
praxis-extproc-image=${PRAXIS_EXTPROC_IMAGE}
maas-api-key-cleanup-image=docker.io/curlimages/curl:latest
monitoring-namespace=
infrastructure-namespace=AUTO
gateway-namespace=${GATEWAY_NAMESPACE}
namespace=${MAAS_NAMESPACE}
ISSUER_REF_NAME=maas-ca-issuer
ISSUER_REF_KIND=Issuer
ISSUER_REF_GROUP=cert-manager.io
EOF

# Select IPP implementation (llm-d vs praxis). Patch the stock xKS overlay
# JSON-patch path and set profile env after apply. Never kubectl-set
# RELATED_IMAGE_* — those use valueFrom→maas-parameters; a literal value
# merged with valueFrom via server-side apply is rejected by the API.
platform_manifests="/maas-api/deploy/overlays/xks"
if [[ "${MAAS_IPP_PROFILE}" == "praxis" ]]; then
  platform_manifests="/maas-api/deploy/overlays/xks-praxis"
fi
sed -i "s|value: /maas-api/deploy/overlays/xks|value: ${platform_manifests}|" \
  "$CTRL_BASE/overlays/xks/kustomization.yaml"

# Force all namespaced objects into MAAS_NAMESPACE (bases default to opendatahub/system).
cat > "$TEMP_DIR/kustomization.yaml" <<EOF
apiVersion: kustomize.config.k8s.io/v1beta1
kind: Kustomization
namespace: ${MAAS_NAMESPACE}
resources:
  - deployment/base/maas-controller/overlays/xks
EOF

# Drop a prior kubectl-set literal so SSA can reinstall ConfigMap valueFrom.
if kc get deployment maas-controller -n "$MAAS_NAMESPACE" &>/dev/null; then
  kc set env deployment/maas-controller -n "$MAAS_NAMESPACE" \
    RELATED_IMAGE_PRAXIS_EXTPROC_IMAGE- >/dev/null 2>&1 || true
fi

MANIFESTS_FILE="$TEMP_DIR/manifests.yaml"
kustomize_build "$TEMP_DIR" > "$MANIFESTS_FILE"

python3 -c "
import sys
with open('$MANIFESTS_FILE') as f:
    content = f.read()
docs = content.split('\n---\n')
filtered = []
for doc in docs:
    if not doc.strip():
        continue
    if 'kind: PodMonitor' in doc or 'kind: ServiceMonitor' in doc:
        continue
    filtered.append(doc)
print('\n---\n'.join(filtered))
" > "$TEMP_DIR/filtered.yaml"

kc apply --server-side=true --force-conflicts -f "$TEMP_DIR/filtered.yaml"

# Overrides manager.yaml default (llm-d). Safe: these env vars use `value:`, not valueFrom.
kc set env deployment/maas-controller -n "$MAAS_NAMESPACE" \
  "MAAS_IPP_PROFILE=${MAAS_IPP_PROFILE}" \
  "MAAS_PLATFORM_MANIFESTS=${platform_manifests}"
ok "maas-controller MAAS_IPP_PROFILE=${MAAS_IPP_PROFILE} MAAS_PLATFORM_MANIFESTS=${platform_manifests}"

kc wait --for=condition=Ready "certificate/maas-controller-webhook-server" \
  -n "$MAAS_NAMESPACE" --timeout=120s
kc rollout status deployment/maas-controller -n "$MAAS_NAMESPACE" --timeout=180s || \
  warn "maas-controller not ready yet"

echo "  Waiting for maas-api..."
for _i in $(seq 1 36); do
  kc get deployment maas-api -n "$MAAS_NAMESPACE" &>/dev/null && break
  sleep 5
done
if kc get deployment maas-api -n "$MAAS_NAMESPACE" &>/dev/null; then
  kc rollout status deployment/maas-api -n "$MAAS_NAMESPACE" --timeout=180s || warn "maas-api not ready"
else
  warn "maas-api not created by controller yet"
fi

echo "  Waiting for payload-processing (IPP)..."
for _i in $(seq 1 36); do
  kc get deployment payload-processing -n "$GATEWAY_NAMESPACE" &>/dev/null && break
  sleep 5
done
if kc get deployment payload-processing -n "$GATEWAY_NAMESPACE" &>/dev/null; then
  kc patch deployment payload-processing -n "$GATEWAY_NAMESPACE" --type=merge \
    -p='{"spec":{"template":{"metadata":{"annotations":{"sidecar.istio.io/inject":"false"}}}}}' 2>/dev/null || true
  kc rollout status deployment/payload-processing -n "$GATEWAY_NAMESPACE" --timeout=180s || \
    warn "payload-processing not ready"
  ok "IPP (payload-processing) reconciled by maas-controller"
else
  warn "payload-processing not created by controller yet"
fi

ok "MaaS platform deploy finished"
