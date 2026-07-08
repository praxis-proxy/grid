#!/usr/bin/env bash
set -euo pipefail

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
#
# Override via environment:
#   KIND_CLUSTER_NAME  cluster name (default: praxis-dev)
#   GWAPI_VERSION      Gateway API CRD version (default: v1.5.1)
#   METALLB_VERSION    MetalLB version (default: v0.14.9)
#
# Projects should copy this file and add project-specific
# deployment functions after the common infrastructure.

CLUSTER_NAME="${KIND_CLUSTER_NAME:-praxis-dev}"
GWAPI_VERSION="${GWAPI_VERSION:-v1.5.1}"
METALLB_VERSION="${METALLB_VERSION:-v0.14.9}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
KUBECTL="kubectl --context kind-${CLUSTER_NAME}"

# ---------------------------------------------------------------------------
# KIND Cluster
# ---------------------------------------------------------------------------

create_cluster() {
    if kind get clusters 2>/dev/null | grep -qx "${CLUSTER_NAME}"; then
        echo "==> Cluster '${CLUSTER_NAME}' already exists, reusing."
    else
        echo "==> Creating KIND cluster '${CLUSTER_NAME}'..."
        kind create cluster \
            --name "${CLUSTER_NAME}" \
            --config "${SCRIPT_DIR}/kind-config.yaml" \
            --wait 60s
    fi
}

# ---------------------------------------------------------------------------
# Gateway API CRDs
# ---------------------------------------------------------------------------

install_gateway_api() {
    echo "==> Installing Gateway API CRDs ${GWAPI_VERSION}..."
    ${KUBECTL} apply -f \
        "https://github.com/kubernetes-sigs/gateway-api/releases/download/${GWAPI_VERSION}/standard-install.yaml"
}

# ---------------------------------------------------------------------------
# MetalLB
# ---------------------------------------------------------------------------

install_metallb() {
    echo "==> Installing MetalLB ${METALLB_VERSION}..."
    ${KUBECTL} apply -f \
        "https://raw.githubusercontent.com/metallb/metallb/${METALLB_VERSION}/config/manifests/metallb-native.yaml"
    ${KUBECTL} wait --namespace metallb-system \
        --for=condition=ready pod \
        --selector=app=metallb \
        --timeout=300s
}

configure_metallb_pool() {
    echo "==> Configuring MetalLB IP pool..."
    SUBNET=$(docker network inspect kind \
        -f '{{range .IPAM.Config}}{{.Subnet}} {{end}}' \
        | tr ' ' '\n' | grep '\.' | head -1)
    IFS='.' read -r a b c d <<< "${SUBNET%%/*}"
    cat <<EOF | ${KUBECTL} apply -f -
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: kind-pool
  namespace: metallb-system
spec:
  addresses:
    - ${a}.${b}.255.200-${a}.${b}.255.210
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: l2
  namespace: metallb-system
EOF
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

for cmd in kind kubectl docker; do
    if ! command -v "${cmd}" &>/dev/null; then
        echo "ERROR: ${cmd} is required but not found"
        exit 1
    fi
done

create_cluster
install_gateway_api
install_metallb
configure_metallb_pool

# Add project-specific deployment functions below:
# load_images
# deploy_project

echo "==> KIND cluster ready."
echo ""
echo "    Cluster:  ${CLUSTER_NAME}"
echo "    Context:  kind-${CLUSTER_NAME}"
