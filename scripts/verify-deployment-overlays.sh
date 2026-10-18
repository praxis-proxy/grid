#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

# The OpenShift flavor carries every cloud's internal keys; each cloud reads only its own.
rendered=$(kustomize build deploy/overlays/openshift | yq eval-all -o=json '[.]')

for service in "grid-operator-swim UDP 7946" "grid-operator-signals TCP 9091"; do
  read -r name protocol port <<< "$service"
  if ! jq -e --arg name "$name" --arg protocol "$protocol" --argjson port "$port" '
    .[] | select(.kind == "Service" and .metadata.name == $name) |
    .spec.type == "LoadBalancer" and
    .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-type"] == "nlb" and
    .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-internal"] == "true" and
    .metadata.annotations["service.beta.kubernetes.io/aws-load-balancer-cross-zone-load-balancing-enabled"] == "true" and
    .metadata.annotations["service.beta.kubernetes.io/azure-load-balancer-internal"] == "true" and
    .metadata.annotations["networking.gke.io/load-balancer-type"] == "Internal" and
    .metadata.annotations["networking.gke.io/internal-load-balancer-allow-global-access"] == "true" and
    (.spec.ports | length) == 1 and
    .spec.ports[0].protocol == $protocol and
    .spec.ports[0].port == $port
  ' <<< "$rendered" > /dev/null; then
    echo "FAIL: $name must request internal load balancers on AWS, Azure and GCP with one $protocol port $port" >&2
    exit 1
  fi

  # targetPort is a name, so the number it resolves to lives in the Deployment.
  if ! jq -e --arg name "$name" --arg protocol "$protocol" --argjson port "$port" '
    (.[] | select(.kind == "Service" and .metadata.name == $name) | .spec.ports[0].targetPort) as $target |
    [
      .[] | select(.kind == "Deployment") | .spec.template.spec.containers[] | (.ports // [])[] |
      select(.name == $target and .containerPort == $port and (.protocol // "TCP") == $protocol)
    ] | length == 1
  ' <<< "$rendered" > /dev/null; then
    echo "FAIL: $name targets a port name no container exposes on $port/$protocol" >&2
    exit 1
  fi
done

# A platform consumer sets the namespace in params.env as well as the kustomization; the
# ServiceMonitor must select and verify the metrics Service in that namespace.
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
cp -r deploy/overlays/openshift "$WORK/moved"
sed -i 's/^namespace=.*/namespace=moved-ns/' "$WORK/moved/params.env"
sed -i 's/^namespace: .*/namespace: moved-ns/' "$WORK/moved/kustomization.yaml"
if ! kustomize build "$WORK/moved" | yq eval-all -o=json '[.]' | jq -e '
  [.[] | select(.kind == "ServiceMonitor")] | length == 1 and
  (.[0] | .metadata.namespace == "moved-ns" and
    .spec.namespaceSelector.matchNames == ["moved-ns"] and
    .spec.endpoints[0].tlsConfig.serverName == "grid-operator-metrics.moved-ns.svc")
' > /dev/null; then
  echo "FAIL: the OpenShift ServiceMonitor must follow the namespace in params.env" >&2
  exit 1
fi

echo "PASS: OpenShift Services request internal load balancers on AWS, Azure and GCP with separate UDP/TCP ports"
echo "PASS: the OpenShift ServiceMonitor follows the namespace in params.env"
