#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT

render() {
  kustomize build "$1" | yq eval-all -o=json '[.]' | \
    jq -S 'sort_by(.apiVersion, .kind, .metadata.namespace, .metadata.name)'
}

render deploy/overlays/openshift > "$WORK/base.json"
render deploy/overlays/openshift-aws > "$WORK/aws.json"

for service in "grid-operator-swim UDP" "grid-operator-signals TCP"; do
  read -r name protocol <<< "$service"
  if ! jq -e --arg name "$name" --arg protocol "$protocol" '
    .[] | select(.kind == "Service" and .metadata.name == $name) |
    .spec.type == "LoadBalancer" and
    .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-type"] == "nlb" and
    (.spec.ports | length) == 1 and .spec.ports[0].protocol == $protocol
  ' "$WORK/aws.json" > /dev/null; then
    echo "FAIL: $name must request an NLB with one $protocol port" >&2
    exit 1
  fi
done

# Comparing every object also protects the inherited TLS, RBAC, and network policy.
jq -S 'map(
  if .kind == "Service" and
    (.metadata.name == "grid-operator-swim" or .metadata.name == "grid-operator-signals")
  then .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-type"] = "nlb"
  else . end
)' "$WORK/base.json" > "$WORK/expected.json"
if ! diff -u "$WORK/expected.json" "$WORK/aws.json"; then
  echo "FAIL: the AWS overlay must change only the two NLB annotations" >&2
  exit 1
fi

echo "PASS: AWS Services request NLBs with separate UDP/TCP ports; all other configuration is inherited"
