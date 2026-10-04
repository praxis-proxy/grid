#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Registers a spoke cluster with the AI Grid fleet dashboard running on the hub.
# Idempotent: re-running with the same arguments changes nothing.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# shellcheck source=lib/registry.sh
# shellcheck disable=SC1091
source "$SCRIPT_DIR/lib/registry.sh"

# Fixed names on the spoke.
SPOKE_NS=aigrid-fleet-metrics
SPOKE_SA=fleet-metrics-reader
SPOKE_SECRET=fleet-metrics-reader-token
SPOKE_CRB=fleet-metrics-reader-cluster-monitoring-view

usage() {
  cat <<'EOF'
Usage: register-site.sh <site-name> --spoke-kubeconfig <path> --hub-kubeconfig <path> --region <code>
                        [--dc <name>] [--lat <n> --lng <n>] [--display-name <s>]
                        [--address <spoke gateway host>] [--entry-namespace <name>]
                        [--namespace aigrid-fleet] [--configmap epp-clusters] [--key clusters.yaml]

Creates a read-only metrics ServiceAccount on the spoke (bound to cluster-monitoring-view),
verifies it can query the spoke's thanos-querier route, stores its token on the hub as
Secret site-<site-name>, and merges the site into the registry ConfigMap.

Requires: oc, curl, and mikefarah yq v4.
EOF
}

log() { printf '==> %s\n' "$*"; }
die() { printf 'error: %s\n' "$*" >&2; exit 1; }

SITE=""; SPOKE_KUBECONFIG=""; HUB_KUBECONFIG=""; REGION=""; DC=""; LAT=""; LNG=""
DISPLAY_NAME=""; ADDRESS=""; ENTRY_NS=clusters
HUB_NS=aigrid-fleet; CM=epp-clusters; KEY=clusters.yaml

while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --spoke-kubeconfig) SPOKE_KUBECONFIG=$2; shift 2 ;;
    --hub-kubeconfig) HUB_KUBECONFIG=$2; shift 2 ;;
    --region) REGION=$2; shift 2 ;;
    --dc) DC=$2; shift 2 ;;
    --lat) LAT=$2; shift 2 ;;
    --lng) LNG=$2; shift 2 ;;
    --display-name) DISPLAY_NAME=$2; shift 2 ;;
    --address) ADDRESS=$2; shift 2 ;;
    --entry-namespace) ENTRY_NS=$2; shift 2 ;;
    --namespace) HUB_NS=$2; shift 2 ;;
    --configmap) CM=$2; shift 2 ;;
    --key) KEY=$2; shift 2 ;;
    --*) die "unknown option: $1" ;;
    *) [[ -z "$SITE" ]] || die "unexpected argument: $1"; SITE=$1; shift ;;
  esac
done

[[ -n "$SITE" ]] || { usage >&2; die "site name is required"; }
[[ "$SITE" =~ ^[a-z0-9]([-a-z0-9]*[a-z0-9])?$ ]] || die "site name must be a DNS-1123 label: $SITE"
[[ -n "$SPOKE_KUBECONFIG" && -r "$SPOKE_KUBECONFIG" ]] || die "--spoke-kubeconfig is required and must be readable"
[[ -n "$HUB_KUBECONFIG" && -r "$HUB_KUBECONFIG" ]] || die "--hub-kubeconfig is required and must be readable"
[[ -n "$REGION" ]] || die "--region is required"
if [[ -n "$LAT" || -n "$LNG" ]]; then
  [[ -n "$LAT" && -n "$LNG" ]] || die "--lat and --lng must be given together"
fi

for tool in oc curl; do
  command -v "$tool" >/dev/null 2>&1 || die "$tool not found in PATH (install the OpenShift CLI: https://mirror.openshift.com/pub/openshift-v4/clients/ocp/stable/)"
done
require_yq

spoke() { oc --kubeconfig "$SPOKE_KUBECONFIG" "$@"; }
hub() { oc --kubeconfig "$HUB_KUBECONFIG" "$@"; }

tmp=$(mktemp -d)
chmod 700 "$tmp"
trap 'rm -rf "$tmp"' EXIT

log "Spoke: $(spoke whoami --show-server)  Hub: $(hub whoami --show-server)"

# 1. Spoke: namespace, ServiceAccount, ClusterRoleBinding, long-lived token Secret.
log "Applying metrics reader resources on the spoke"
spoke apply -f - <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $SPOKE_NS
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: $SPOKE_SA
  namespace: $SPOKE_NS
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRoleBinding
metadata:
  name: $SPOKE_CRB
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: cluster-monitoring-view
subjects:
  - kind: ServiceAccount
    name: $SPOKE_SA
    namespace: $SPOKE_NS
---
apiVersion: v1
kind: Secret
metadata:
  name: $SPOKE_SECRET
  namespace: $SPOKE_NS
  annotations:
    kubernetes.io/service-account.name: $SPOKE_SA
type: kubernetes.io/service-account-token
EOF

# 2. Wait for the token controller to populate the Secret.
log "Waiting for the ServiceAccount token"
TOKEN=""
for _ in $(seq 1 30); do
  TOKEN=$(spoke -n "$SPOKE_NS" get secret "$SPOKE_SECRET" -o jsonpath='{.data.token}' 2>/dev/null | base64 -d || true)
  [[ -n "$TOKEN" ]] && break
  sleep 2
done
[[ -n "$TOKEN" ]] || die "token was not populated in $SPOKE_NS/$SPOKE_SECRET after 60s"
printf '%s' "$TOKEN" >"$tmp/token"
chmod 600 "$tmp/token"

# 3. Thanos querier route.
THANOS_HOST=$(spoke -n openshift-monitoring get route thanos-querier -o jsonpath='{.spec.host}')
[[ -n "$THANOS_HOST" ]] || die "route thanos-querier not found in openshift-monitoring on the spoke"
METRICS_URL="https://$THANOS_HOST"

# 4. Verify the token works and decide whether a CA bundle must be stored.
# The token is written to a curl config file (mode 600 inside the mode 700
# temp dir), never passed on the command line, so it never appears in argv
# (and thus never in /proc/<pid>/cmdline) for the duration of the probes.
CURLRC="$tmp/curlrc"
( umask 077; printf 'header = "Authorization: Bearer %s"\n' "$TOKEN" >"$CURLRC" )
probe() { # probe [extra curl args...]
  curl -sS --max-time 15 -K "$CURLRC" "$@" \
    "$METRICS_URL/api/v1/query?query=up" 2>/dev/null | grep -q '"status":"success"'
}
# The pod runs on ubi-minimal with only the public CA bundle, and
# internal/metrics/client.go appends ca.crt to that pool rather than
# replacing it, so storing a cluster CA is never harmful. Probe in the order
# the pod should trust:
#   Tier 1: default-ingress-cert (openshift-config-managed) - the CA that
#           actually signs this route's certificate on most clusters.
#   Tier 2: kube-root-ca.crt (in the spoke namespace) - the cluster's
#           internal CA, for routes signed by a custom ingress certificate.
#   Tier 3: the system trust store - the route is signed by a public CA, so
#           nothing needs to be stored.
CA_FILE=""
log "Verifying $METRICS_URL/api/v1/query?query=up"
spoke -n openshift-config-managed get configmap default-ingress-cert -o jsonpath='{.data.ca-bundle\.crt}' >"$tmp/default-ingress-ca.crt" 2>/dev/null || true
spoke -n "$SPOKE_NS" get configmap kube-root-ca.crt -o jsonpath='{.data.ca\.crt}' >"$tmp/kube-root-ca.crt" 2>/dev/null || true
for candidate in "$tmp/default-ingress-ca.crt" "$tmp/kube-root-ca.crt"; do
  if [[ -s "$candidate" ]] && probe --cacert "$candidate"; then
    CA_FILE="$candidate"
    log "TLS verified with $(basename "$candidate"); it will be stored as ca.crt"
    break
  fi
done
if [[ -z "$CA_FILE" ]]; then
  if probe; then
    log "TLS verified with the system trust store; the route is signed by a public CA, so no ca.crt will be stored"
  elif probe -k; then
    die "token is valid but the route certificate is not signed by the system trust store, kube-root-ca.crt, or default-ingress-cert; add the signing CA to the ConfigMap default-ingress-cert or re-run after fixing the ingress certificate"
  else
    die "query against $METRICS_URL failed: check the ClusterRoleBinding $SPOKE_CRB and that thanos-querier is reachable from this machine"
  fi
fi

# 5. Hub: namespace and site Secret.
log "Storing Secret site-$SITE in $HUB_NS on the hub"
hub create namespace "$HUB_NS" --dry-run=client -o yaml | hub apply -f - >/dev/null
secret_args=(--from-file=token="$tmp/token")
[[ -n "$CA_FILE" ]] && secret_args+=(--from-file=ca.crt="$CA_FILE")
hub -n "$HUB_NS" create secret generic "site-$SITE" "${secret_args[@]}" --dry-run=client -o yaml | hub apply -f -

# 6. Build the registry entry.
if [[ -z "$ADDRESS" ]]; then
  ADDRESS="gateway.${THANOS_HOST#thanos-querier-openshift-monitoring.}"
fi
SITE_NAME="$SITE" ENTRY_NS="$ENTRY_NS" ADDRESS="$ADDRESS" REGION="$REGION" \
METRICS_URL="$METRICS_URL" METRICS_SECRET="site-$SITE" \
yq -n '
  .name = strenv(SITE_NAME) |
  .namespace = strenv(ENTRY_NS) |
  .address = strenv(ADDRESS) |
  .port = "443" |
  .labels.region = strenv(REGION) |
  .labels.metricsURL = strenv(METRICS_URL) |
  .labels.metricsSecret = strenv(METRICS_SECRET)
' >"$tmp/entry.yaml"
[[ -n "$DC" ]] && V="$DC" yq -i '.labels.dc = strenv(V)' "$tmp/entry.yaml"
[[ -n "$LAT" ]] && V="$LAT" yq -i '.labels.lat = strenv(V)' "$tmp/entry.yaml"
[[ -n "$LNG" ]] && V="$LNG" yq -i '.labels.lng = strenv(V)' "$tmp/entry.yaml"
[[ -n "$DISPLAY_NAME" ]] && V="$DISPLAY_NAME" yq -i '.labels.displayName = strenv(V)' "$tmp/entry.yaml"

# 7. Merge into the registry ConfigMap.
KEY_JSONPATH="{.data.${KEY//./\\.}}"
if hub -n "$HUB_NS" get configmap "$CM" >/dev/null 2>&1; then
  hub -n "$HUB_NS" get configmap "$CM" -o jsonpath="$KEY_JSONPATH" >"$tmp/clusters.yaml"
  merge_site_entry "$tmp/clusters.yaml" "$tmp/entry.yaml"
  KEY="$KEY" FILE="$tmp/clusters.yaml" yq -n -o=json '.data[strenv(KEY)] = load_str(strenv(FILE))' >"$tmp/patch.json"
  log "Merging entry into ConfigMap $CM"
  hub -n "$HUB_NS" patch configmap "$CM" --type merge --patch-file "$tmp/patch.json"
else
  : >"$tmp/clusters.yaml"
  merge_site_entry "$tmp/clusters.yaml" "$tmp/entry.yaml"
  log "Creating ConfigMap $CM"
  hub -n "$HUB_NS" create configmap "$CM" --from-file="$KEY=$tmp/clusters.yaml"
fi

log "Registered $SITE"
cat "$tmp/entry.yaml"
