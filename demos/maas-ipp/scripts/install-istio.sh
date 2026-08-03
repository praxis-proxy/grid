#!/usr/bin/env bash
# Install Istio minimal profile with Gateway API Inference Extension enabled.
# Uses a fetched istioctl matching ISTIO_VERSION (default from lib.sh), not PATH.
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_cmd kubectl
ensure_istioctl

installed_ver=""
if kc get deployment istiod -n istio-system &>/dev/null; then
  installed_ver="$(
    kc -n istio-system get deploy istiod -o jsonpath='{.spec.template.spec.containers[0].image}' 2>/dev/null \
      | sed -n 's/.*://p' || true
  )"
  if [[ "$installed_ver" == "$ISTIO_VERSION" ]]; then
    ok "Istio ${ISTIO_VERSION} already installed"
    exit 0
  fi
  warn "Istio image tag '${installed_ver:-unknown}' != ${ISTIO_VERSION}; upgrading"
fi

"$ISTIOCTL" install --context "$KUBE_CONTEXT" --set profile=minimal \
  --set values.pilot.env.SUPPORT_GATEWAY_API_INFERENCE_EXTENSION=true \
  --set values.pilot.env.ENABLE_GATEWAY_API_INFERENCE_EXTENSION=true \
  -y
kc rollout status deployment/istiod -n istio-system --timeout=180s
ok "Istio ${ISTIO_VERSION} installed (GIE enabled)"
