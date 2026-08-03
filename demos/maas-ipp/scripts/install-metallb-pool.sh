#!/usr/bin/env bash
# Configure MetalLB IPAddressPool from the Kind docker network (single-cluster).
# metallb-auto-pool requires crossCluster; this lab does not.
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_cmd docker

if kc get ipaddresspool forge-pool -n metallb-system &>/dev/null; then
  ok "MetalLB pool already configured"
  exit 0
fi

KIND_SUBNET=$(docker network inspect kind -f '{{range .IPAM.Config}}{{.Subnet}} {{end}}' | tr ' ' '\n' | grep '\.' | head -1)
[[ -n "$KIND_SUBNET" ]] || die "could not determine Kind docker network IPv4 subnet"
LB_BASE=$(echo "$KIND_SUBNET" | cut -d'.' -f1-3)

_retries=0
while [[ $_retries -lt 6 ]]; do
  if kc apply -f - <<EOF 2>/dev/null
apiVersion: metallb.io/v1beta1
kind: IPAddressPool
metadata:
  name: forge-pool
  namespace: metallb-system
spec:
  addresses:
  - ${LB_BASE}.200-${LB_BASE}.250
---
apiVersion: metallb.io/v1beta1
kind: L2Advertisement
metadata:
  name: forge-l2
  namespace: metallb-system
EOF
  then
    ok "MetalLB pool ${LB_BASE}.200-250"
    exit 0
  fi
  echo "  MetalLB webhook not ready, retrying..."
  sleep 10
  _retries=$((_retries + 1))
done
die "failed to configure MetalLB pool"
