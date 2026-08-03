#!/usr/bin/env bash
# Rebuild/reload maas-controller (and optionally maas-api) into the Kind cluster.
# Used when iterating on controller changes (e.g. IPP → Praxis via MAAS_IPP_PROFILE).
# Forge does not own EnvoyFilter YAML — only the controller image does.
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_maas_root
require_cmd kind
require_cmd docker

COMPONENT="${2:-maas-controller}"

# kind load + Deployment imagePullPolicy=Always + a quay.io tag makes the kubelet
# re-pull the remote digest and discard the locally loaded image. Retag to a
# non-registry localhost/ name so Always cannot fetch a replacement.
prefer_local_kind_tag() {
  local image="$1"
  local name="$2"
  case "$image" in
    quay.io/opendatahub/"${name}":*)
      local tag="${image##*:}"
      echo "localhost/${name}:${tag}"
      ;;
    *)
      echo "$image"
      ;;
  esac
}

# Point the Deployment at the kind-loaded image and stop Always from re-pulling.
pin_deployment_image() {
  local deploy="$1"
  local container="$2"
  local image="$3"
  local ns="${4:-$MAAS_NAMESPACE}"

  kc set image "deployment/${deploy}" -n "$ns" "${container}=${image}" || true
  kc patch "deployment/${deploy}" -n "$ns" --type=json -p="[
    {\"op\":\"replace\",\"path\":\"/spec/template/spec/containers/0/imagePullPolicy\",\"value\":\"IfNotPresent\"}
  ]"
  # Ensure a new ReplicaSet even if only pullPolicy changed.
  kc rollout restart "deployment/${deploy}" -n "$ns"
  kc rollout status "deployment/${deploy}" -n "$ns" --timeout=180s
}

case "$COMPONENT" in
  maas-controller|controller)
    MAAS_CONTROLLER_IMAGE="$(prefer_local_kind_tag "$MAAS_CONTROLLER_IMAGE" maas-controller)"
    echo "  Building maas-controller → ${MAAS_CONTROLLER_IMAGE}"
    (cd "$MAAS_ROOT" && docker build -f maas-controller/Dockerfile -t "$MAAS_CONTROLLER_IMAGE" .)
    kind load docker-image "$MAAS_CONTROLLER_IMAGE" --name "$KIND_CLUSTER_NAME"
    platform_manifests="/maas-api/deploy/overlays/xks"
    if [[ "${MAAS_IPP_PROFILE}" == "praxis" ]]; then
      platform_manifests="/maas-api/deploy/overlays/xks-praxis"
    fi
    kc set env deployment/maas-controller -n "$MAAS_NAMESPACE" \
      "MAAS_IPP_PROFILE=${MAAS_IPP_PROFILE}" \
      "MAAS_PLATFORM_MANIFESTS=${platform_manifests}" \
      "RELATED_IMAGE_PRAXIS_EXTPROC_IMAGE=${PRAXIS_EXTPROC_IMAGE}"
    pin_deployment_image maas-controller manager "$MAAS_CONTROLLER_IMAGE"
    ok "maas-controller reloaded (${MAAS_CONTROLLER_IMAGE}, profile=${MAAS_IPP_PROFILE}, manifests=${platform_manifests})"
    ;;
  maas-api|api)
    MAAS_API_IMAGE="$(prefer_local_kind_tag "$MAAS_API_IMAGE" maas-api)"
    echo "  Building maas-api → ${MAAS_API_IMAGE}"
    (cd "$MAAS_ROOT/maas-api" && docker build -t "$MAAS_API_IMAGE" .)
    kind load docker-image "$MAAS_API_IMAGE" --name "$KIND_CLUSTER_NAME"
    pin_deployment_image maas-api '*' "$MAAS_API_IMAGE"
    ok "maas-api reloaded (${MAAS_API_IMAGE}, imagePullPolicy=IfNotPresent)"
    ;;
  *)
    die "unknown component '$COMPONENT' (use maas-controller|maas-api)"
    ;;
esac
