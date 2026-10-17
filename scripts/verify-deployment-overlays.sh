#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The OpenShift flavor carries the AWS annotations, which other clouds ignore.
rendered=$(kustomize build deploy/overlays/openshift | yq eval-all -o=json '[.]')

for service in "grid-operator-swim UDP" "grid-operator-signals TCP"; do
  read -r name protocol <<< "$service"
  if ! jq -e --arg name "$name" --arg protocol "$protocol" '
    .[] | select(.kind == "Service" and .metadata.name == $name) |
    .spec.type == "LoadBalancer" and
    .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-type"] == "nlb" and
    .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-internal"] == "true" and
    .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled"] == "true" and
    (.spec.ports | length) == 1 and .spec.ports[0].protocol == $protocol
  ' <<< "$rendered" > /dev/null; then
    echo "FAIL: $name must request an internal cross-zone NLB with one $protocol port" >&2
    exit 1
  fi
done

echo "PASS: OpenShift Services request internal cross-zone NLBs with separate UDP/TCP ports"
