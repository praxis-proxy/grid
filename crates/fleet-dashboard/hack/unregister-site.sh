#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Removes a spoke from the fleet dashboard registry on the hub and, when a spoke
# kubeconfig is given, deletes the metrics reader resources on the spoke.
# Idempotent: re-running when nothing is left is a no-op.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib/registry.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/registry.sh"

SPOKE_NS=aigrid-fleet-metrics
SPOKE_CRB=fleet-metrics-reader-cluster-monitoring-view

usage() {
  cat <<'EOF'
Usage: unregister-site.sh <site-name> --hub-kubeconfig <path> [--spoke-kubeconfig <path>]
                          [--namespace aigrid-fleet] [--configmap epp-clusters] [--key clusters.yaml]

Removes the site's entry from the registry ConfigMap and deletes Secret site-<site-name> on the hub.
With --spoke-kubeconfig, also deletes namespace aigrid-fleet-metrics and the ClusterRoleBinding on the spoke.
EOF
}

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

SITE=""; SPOKE_KUBECONFIG=""; HUB_KUBECONFIG=""
HUB_NS=aigrid-fleet; CM=epp-clusters; KEY=clusters.yaml

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --spoke-kubeconfig) SPOKE_KUBECONFIG=$2; shift 2 ;;
    --hub-kubeconfig) HUB_KUBECONFIG=$2; shift 2 ;;
    --namespace) HUB_NS=$2; shift 2 ;;
    --configmap) CM=$2; shift 2 ;;
    --key) KEY=$2; shift 2 ;;
    --*) die "unknown option: $1" ;;
    *) [[ -z "$SITE" ]] || die "unexpected argument: $1"; SITE=$1; shift ;;
  esac
done

[[ -n "$SITE" ]] || { usage >&2; die "site name is required"; }
[[ -n "$HUB_KUBECONFIG" && -r "$HUB_KUBECONFIG" ]] || die "--hub-kubeconfig is required and must be readable"
command -v oc >/dev/null 2>&1 || die "oc not found in PATH"
require_yq

hub() { oc --kubeconfig "$HUB_KUBECONFIG" "$@"; }

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT

KEY_JSONPATH="{.data.${KEY//./\\.}}"
if hub -n "$HUB_NS" get configmap "$CM" >/dev/null 2>&1; then
  # A ConfigMap missing this key has no registry data to edit; patching it
  # anyway would write data[KEY]="" and create the key with an empty value.
  HAS_KEY=$(hub -n "$HUB_NS" get configmap "$CM" -o json | KEY="$KEY" yq '(.data // {}) | has(strenv(KEY))')
  if [[ "$HAS_KEY" == "true" ]]; then
    hub -n "$HUB_NS" get configmap "$CM" -o jsonpath="$KEY_JSONPATH" >"$tmp/clusters.yaml"
    remove_site_entry "$tmp/clusters.yaml" "$SITE"
    KEY="$KEY" FILE="$tmp/clusters.yaml" yq -n -o=json '.data[strenv(KEY)] = load_str(strenv(FILE))' >"$tmp/patch.json"
    log "Removing $SITE from ConfigMap $CM"
    hub -n "$HUB_NS" patch configmap "$CM" --type merge --patch-file "$tmp/patch.json"
  else
    log "registry key $KEY not present; nothing to remove"
  fi
else
  log "ConfigMap $CM not found in $HUB_NS; nothing to remove from the registry"
fi

log "Deleting Secret site-$SITE"
hub -n "$HUB_NS" delete secret "site-$SITE" --ignore-not-found

if [[ -n "$SPOKE_KUBECONFIG" ]]; then
  [[ -r "$SPOKE_KUBECONFIG" ]] || die "--spoke-kubeconfig is not readable"
  spoke() { oc --kubeconfig "$SPOKE_KUBECONFIG" "$@"; }
  log "Deleting metrics reader resources on the spoke $(spoke whoami --show-server)"
  spoke delete clusterrolebinding "$SPOKE_CRB" --ignore-not-found
  spoke delete namespace "$SPOKE_NS" --ignore-not-found --wait=false
fi

log "Unregistered $SITE"
