#!/usr/bin/env bash
# Tests issuance and CA retention on a disposable cluster, with no Grid runtime.
set -Eeuo pipefail
umask 077

CHART=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CLUSTER="${CLUSTER:-grid-bootstrap-test}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.21.3}"
WORK=$(mktemp -d)
CREATED=false
export KUBECONFIG="$WORK/kubeconfig"

cleanup() {
  if [[ "$CREATED" == true ]]; then
    kind delete cluster --name "$CLUSTER"
  fi
  rm -rf "$WORK"
}
trap cleanup EXIT

if kind get clusters | grep -Fx "$CLUSTER" >/dev/null; then
  echo "Refusing to reuse existing cluster $CLUSTER" >&2
  exit 1
fi
CREATED=true
kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" --wait 120s
kubectl apply --server-side -f \
  "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
for deployment in cert-manager cert-manager-cainjector cert-manager-webhook; do
  kubectl -n cert-manager rollout status "deployment/$deployment" --timeout=180s
done
kubectl create namespace fresh
kubectl create namespace existing

secret_digest() {
  kubectl -n "$1" get secret grid-ca-key -o json | jq -c .data | sha256sum
}

issue_leaf() {
  local namespace=$1
  kubectl -n "$namespace" apply -f - <<'YAML'
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: site-identity
spec:
  secretName: site-identity
  commonName: site-a
  subject:
    organizations: [site-a]
  uris: [spiffe://grid.internal/site/site-a]
  dnsNames: [site-a.grid.internal]
  duration: 168h
  renewBefore: 72h
  privateKey:
    algorithm: ECDSA
    size: 256
    encoding: PKCS8
    rotationPolicy: Always
  usages: [digital signature, client auth, server auth]
  issuerRef:
    name: grid-ca
    kind: Issuer
    group: cert-manager.io
YAML
  kubectl -n "$namespace" wait certificate/site-identity --for=condition=Ready --timeout=120s
  kubectl -n "$namespace" get secret site-identity -o jsonpath='{.data.tls\.crt}' | base64 -d > "$WORK/leaf.crt"
  openssl verify -CAfile "$WORK/ca.crt" -purpose sslclient "$WORK/leaf.crt"
  openssl verify -CAfile "$WORK/ca.crt" -purpose sslserver "$WORK/leaf.crt"
}

helm upgrade --install bootstrap "$CHART" -n fresh --set enabled=true,ca.mode=Generate
kubectl -n fresh wait certificate/grid-root-ca --for=condition=Ready --timeout=120s
kubectl -n fresh wait issuer/grid-ca --for=condition=Ready --timeout=120s
kubectl -n fresh get secret grid-ca-key -o jsonpath='{.data.tls\.crt}' | base64 -d > "$WORK/ca.crt"
before=$(secret_digest fresh)
issue_leaf fresh

helm upgrade bootstrap "$CHART" -n fresh --set enabled=true,ca.mode=Generate
[[ "$(secret_digest fresh)" == "$before" ]]
kubectl -n fresh patch certificate site-identity --type=merge \
  -p '{"spec":{"dnsNames":["site-a.grid.internal","site-a-signals.grid.internal"]}}'
kubectl -n fresh wait certificate/site-identity --for=jsonpath='{.status.revision}'=2 --timeout=120s
[[ "$(secret_digest fresh)" == "$before" ]]
echo "PASS: fresh bootstrap issues site certificates, reissues a leaf, and preserves the CA on upgrade"

# Copy only this test's generated CA into the adoption fixture.
kubectl -n fresh get secret grid-ca-key -o json | \
  jq '{apiVersion:"v1",kind:"Secret",metadata:{name:"grid-ca-key",namespace:"existing"},type:"kubernetes.io/tls",data:.data}' | \
  kubectl apply -f -
existing_before=$(secret_digest existing)
helm upgrade --install bootstrap "$CHART" -n existing --set enabled=true
kubectl -n existing wait issuer/grid-ca --for=condition=Ready --timeout=120s
issue_leaf existing
helm upgrade bootstrap "$CHART" -n existing --set enabled=true
[[ "$(secret_digest existing)" == "$existing_before" ]]
helm uninstall bootstrap -n existing
[[ "$(secret_digest existing)" == "$existing_before" ]]
echo "PASS: existing CA signs leaves and survives upgrades and uninstall unchanged"

helm uninstall bootstrap -n fresh
kubectl -n fresh get certificate/grid-root-ca issuer/grid-root-selfsigned secret/grid-ca-key >/dev/null
[[ "$(secret_digest fresh)" == "$before" ]]
echo "PASS: uninstall retains the generated CA and its bootstrap issuer"
