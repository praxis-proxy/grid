#!/usr/bin/env bash
# Install Gateway API Inference Extension CRDs (GIE).
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_cmd kubectl

if kc get crd inferencepools.inference.networking.k8s.io &>/dev/null; then
  ok "GIE CRDs already installed"
  exit 0
fi

kustomize_build "github.com/kubernetes-sigs/gateway-api-inference-extension/config/crd?ref=${GIE_VERSION}" \
  | kc apply -f -
ok "GIE CRDs ${GIE_VERSION} installed"
