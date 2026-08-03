#!/usr/bin/env bash
# Shared helpers for demos/maas-ipp scripts.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DEMO_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Version pins (override via env)
ISTIO_VERSION="${ISTIO_VERSION:-1.30.3}"
GATEWAY_API_VERSION="${GATEWAY_API_VERSION:-1.5.1}"
GIE_VERSION="${GIE_VERSION:-v1.5.0}"
KUADRANT_VERSION="${KUADRANT_VERSION:-1.3.1}"
CERTMANAGER_VERSION="${CERTMANAGER_VERSION:-1.17.2}"
METALLB_VERSION="${METALLB_VERSION:-v0.14.9}"

MAAS_NAMESPACE="${MAAS_NAMESPACE:-maas-system}"
GATEWAY_NAMESPACE="${GATEWAY_NAMESPACE:-istio-system}"
SUBSCRIPTION_NAMESPACE="${SUBSCRIPTION_NAMESPACE:-models-as-a-service}"

MAAS_API_IMAGE="${MAAS_API_IMAGE:-quay.io/opendatahub/maas-api:latest}"
MAAS_CONTROLLER_IMAGE="${MAAS_CONTROLLER_IMAGE:-quay.io/opendatahub/maas-controller:latest}"
IPP_IMAGE="${IPP_IMAGE:-quay.io/opendatahub/odh-ai-gateway-payload-processing:odh-stable}"
# Controller MAAS_IPP_PROFILE: llm-d (stock IPP) or praxis (payload-processing-praxis overlay).
# Requires MAAS_ROOT with MAAS_IPP_PROFILE support (models-as-a-service).
MAAS_IPP_PROFILE="${MAAS_IPP_PROFILE:-praxis}"
PRAXIS_EXTPROC_IMAGE="${PRAXIS_EXTPROC_IMAGE:-praxis-extproc:dev}"
LLMISVC_IMAGE="${LLMISVC_IMAGE:-quay.io/opendatahub/odh-kserve-llmisvc-controller:odh-stable}"
# Must be a revision with separate llmisvc controller + v1alpha2 CRDs
# (config/llmisvc/manager.yaml and config/crd/full/llmisvc/).
# Older ODH pins (e.g. odh-v3.3 / 47894470ea49) only ship v1alpha1 and nest
# configs under config/llmisvc/ — incompatible with the Kind install path.
KSERVE_COMMIT="${KSERVE_COMMIT:-174dfeabf6eba6c459ee0ba32ebbdb0e2e1b7033}"
KSERVE_CLONE="${KSERVE_CLONE:-/tmp/opendatahub-kserve}"

BOLD='\033[1m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
RED='\033[0;31m'
NC='\033[0m'

ok() { echo -e "  ${GREEN}OK${NC} $1"; }
warn() { echo -e "  ${YELLOW}!${NC} $1"; }
fail() { echo -e "  ${RED}FAIL${NC} $1" >&2; }
die() { fail "$1"; exit 1; }

require_cmd() {
  command -v "$1" >/dev/null 2>&1 || die "missing required tool: $1"
}

# Resolve OS/arch for Istio release tarballs (linux-amd64 / linux-arm64).
istio_release_arch() {
  local os arch
  os="$(uname -s | tr '[:upper:]' '[:lower:]')"
  case "$(uname -m)" in
    x86_64|amd64) arch=amd64 ;;
    aarch64|arm64) arch=arm64 ;;
    *) die "unsupported architecture for istioctl: $(uname -m)" ;;
  esac
  [[ "$os" == "linux" || "$os" == "darwin" ]] || die "unsupported OS for istioctl: $os"
  echo "${os}-${arch}"
}

# Download and cache istioctl matching ISTIO_VERSION. Sets ISTIOCTL to the binary path.
# Override cache with ISTIOCTL_CACHE_DIR. Requires curl (or wget) and tar.
ensure_istioctl() {
  local cache_root="${ISTIOCTL_CACHE_DIR:-${DEMO_DIR}/.cache}"
  local dest="${cache_root}/istio-${ISTIO_VERSION}"
  local bin="${dest}/bin/istioctl"
  local platform tarball url tmp

  if [[ -x "$bin" ]]; then
    if "$bin" version --remote=false 2>/dev/null | grep -q "$ISTIO_VERSION"; then
      ISTIOCTL="$bin"
      export ISTIOCTL
      return 0
    fi
  fi

  require_cmd tar
  platform="$(istio_release_arch)"
  tarball="istio-${ISTIO_VERSION}-${platform}.tar.gz"
  url="https://github.com/istio/istio/releases/download/${ISTIO_VERSION}/${tarball}"
  tmp="$(mktemp -d)"
  echo "  Fetching istioctl ${ISTIO_VERSION} (${platform})..."
  if command -v curl >/dev/null 2>&1; then
    curl -fsSL "$url" -o "${tmp}/${tarball}"
  elif command -v wget >/dev/null 2>&1; then
    wget -q -O "${tmp}/${tarball}" "$url"
  else
    rm -rf "$tmp"
    die "missing required tool: curl or wget (to fetch istioctl ${ISTIO_VERSION})"
  fi
  mkdir -p "$dest"
  tar -xzf "${tmp}/${tarball}" -C "$tmp"
  # Tarball root is istio-${VERSION}/bin/istioctl
  if [[ ! -f "${tmp}/istio-${ISTIO_VERSION}/bin/istioctl" ]]; then
    rm -rf "$tmp"
    die "istio tarball missing bin/istioctl"
  fi
  mkdir -p "${dest}/bin"
  cp -f "${tmp}/istio-${ISTIO_VERSION}/bin/istioctl" "$bin"
  chmod +x "$bin"
  rm -rf "$tmp"

  if ! "$bin" version --remote=false 2>/dev/null | grep -q "$ISTIO_VERSION"; then
    die "downloaded istioctl does not report version ${ISTIO_VERSION}"
  fi
  ISTIOCTL="$bin"
  export ISTIOCTL
  ok "istioctl ${ISTIO_VERSION} ready at ${bin}"
}

# Usage: parse_context "$@" → sets KUBE_CONTEXT and KIND_CLUSTER_NAME
parse_context() {
  if [[ $# -lt 1 || -z "${1:-}" ]]; then
    die "usage: $0 <kube-context>  (e.g. kind-maas-ipp-local)"
  fi
  KUBE_CONTEXT="$1"
  if [[ "$KUBE_CONTEXT" == kind-* ]]; then
    KIND_CLUSTER_NAME="${KUBE_CONTEXT#kind-}"
  else
    KIND_CLUSTER_NAME="${KIND_CLUSTER_NAME:-maas-ipp-local}"
  fi
  export KUBE_CONTEXT KIND_CLUSTER_NAME
  KUBECTL=(kubectl --context "$KUBE_CONTEXT")
}

require_maas_root() {
  if [[ -z "${MAAS_ROOT:-}" ]]; then
    die "MAAS_ROOT is unset. Export it to your models-as-a-service checkout."
  fi
  if [[ ! -d "$MAAS_ROOT/deployment/base/maas-controller" ]]; then
    die "MAAS_ROOT=$MAAS_ROOT does not look like a models-as-a-service checkout"
  fi
  MAAS_ROOT="$(cd "$MAAS_ROOT" && pwd)"
  export MAAS_ROOT
}

kc() {
  "${KUBECTL[@]}" "$@"
}

# Prefer standalone kustomize; fall back to kubectl's built-in.
kustomize_build() {
  local dir="$1"
  if command -v kustomize >/dev/null 2>&1; then
    kustomize build --load-restrictor LoadRestrictionsNone "$dir"
  else
    kubectl kustomize --load-restrictor=LoadRestrictionsNone "$dir"
  fi
}

# Clone/update opendatahub-io/kserve and check out KSERVE_COMMIT.
ensure_kserve_clone() {
  require_cmd git
  if [[ ! -d "$KSERVE_CLONE/.git" ]]; then
    require_cmd gh
    echo "  Cloning opendatahub-io/kserve into ${KSERVE_CLONE}..."
    gh repo clone opendatahub-io/kserve "$KSERVE_CLONE"
  fi
  echo "  Checking out kserve ${KSERVE_COMMIT}..."
  git -C "$KSERVE_CLONE" fetch --tags origin "$KSERVE_COMMIT"
  git -C "$KSERVE_CLONE" checkout --force --detach "$KSERVE_COMMIT"
  if [[ ! -f "$KSERVE_CLONE/config/llmisvc/manager.yaml" ]]; then
    die "KSERVE_COMMIT=${KSERVE_COMMIT} has no config/llmisvc/manager.yaml (need a separate-controller revision)"
  fi
  if [[ ! -d "$KSERVE_CLONE/config/crd/full/llmisvc" ]]; then
    die "KSERVE_COMMIT=${KSERVE_COMMIT} has no config/crd/full/llmisvc (need v1alpha2-capable CRDs)"
  fi
}

# ODH distro RBAC (DestinationRule + OpenShift Route watch). Vanilla
# config/llmisvc omits these; without them the manager fails cache sync and
# restarts, taking the mutating webhook down with it.
apply_llmisvc_distro_rbac() {
  local role_dir="$KSERVE_CLONE/config/overlays/odh/rbac/llmisvc"
  [[ -f "$role_dir/role.yaml" && -f "$role_dir/clusterrolebinding.yaml" ]] || \
    die "missing ODH llmisvc RBAC under ${role_dir}"
  kc apply -f "$role_dir/role.yaml"
  kc apply -f "$role_dir/clusterrolebinding.yaml"
  # Controller binds per-namespace SAs to this ClusterRole during monitoring reconcile.
  local metrics_rbac="$KSERVE_CLONE/config/monitoring/llmisvc/rbac.yaml"
  [[ -f "$metrics_rbac" ]] || die "missing llmisvc metrics RBAC at ${metrics_rbac}"
  kc apply -f "$metrics_rbac"
}

# Kind defaults (100m/300Mi) are too tight for llmisvc cache sync; the
# container dies on liveness before the webhook stays up.
patch_llmisvc_resources() {
  kc -n kserve patch deployment llmisvc-controller-manager --type=strategic -p '{
    "spec": {
      "template": {
        "spec": {
          "containers": [{
            "name": "manager",
            "resources": {
              "requests": {"cpu": "200m", "memory": "512Mi"},
              "limits": {"cpu": "1", "memory": "1Gi"}
            }
          }]
        }
      }
    }
  }'
}

# ODH llmisvc signs workload certs with OpenShift service-CA by default.
# Kind has no openshift-service-ca/signing-key — point it at the cert-manager
# CA already provisioned for MaaS (maas-root-ca).
patch_llmisvc_signing_ca() {
  kc -n kserve set env deployment/llmisvc-controller-manager \
    "SERVICE_CA_SIGNING_SECRET_NAMESPACE=${MAAS_NAMESPACE}" \
    "SERVICE_CA_SIGNING_SECRET_NAME=maas-root-ca"
}

# Wait until a CRD is Established. Retries past the race where kubectl wait
# fails with ".status.conditions accessor error: <nil>" right after apply.
wait_for_crd_established() {
  local crd="$1"
  local timeout_s="${2:-120}"
  local optional="${3:-}"
  local deadline=$((SECONDS + timeout_s))
  local err=""

  echo "  Waiting for CRD ${crd} Established..."
  while (( SECONDS < deadline )); do
    if ! kc get "crd/${crd}" &>/dev/null; then
      sleep 2
      continue
    fi
    # Skip wait until conditions exist; bare kubectl wait races on nil status.
    if [[ -z "$(kc get "crd/${crd}" -o jsonpath='{.status.conditions}' 2>/dev/null || true)" ]]; then
      sleep 2
      continue
    fi
    if err=$(kc wait --for=condition=Established "crd/${crd}" --timeout=15s 2>&1); then
      ok "CRD ${crd} Established"
      return 0
    fi
    # Retry known race; surface other errors but keep polling until deadline.
    if [[ "$err" == *'status.conditions accessor error'* ]] || \
       [[ "$err" == *'NotFound'* ]] || \
       [[ "$err" == *'timed out'* ]] || \
       [[ "$err" == *'context deadline'* ]]; then
      sleep 2
      continue
    fi
    sleep 2
  done

  if [[ "$optional" == "optional" ]]; then
    warn "CRD ${crd} not Established within ${timeout_s}s (optional)"
    return 0
  fi
  die "CRD ${crd} not Established within ${timeout_s}s${err:+: ${err}}"
}

# Wait until the LLMIS mutating webhook answers (server-side dry-run).
wait_for_llmisvc_webhook() {
  local ns="${1:-default}"
  local attempts="${2:-36}"
  local i=0
  echo "  Waiting for llmisvc webhook..."
  while [[ $i -lt $attempts ]]; do
    if kc apply --dry-run=server -f - <<EOF >/dev/null 2>&1
apiVersion: serving.kserve.io/v1alpha2
kind: LLMInferenceService
metadata:
  name: webhook-readiness-probe
  namespace: ${ns}
spec:
  model:
    uri: hf://placeholder/no-model
    name: probe
  replicas: 1
EOF
    then
      ok "llmisvc webhook ready"
      return 0
    fi
    sleep 5
    i=$((i + 1))
  done
  die "llmisvc webhook not ready after $((attempts * 5))s"
}
