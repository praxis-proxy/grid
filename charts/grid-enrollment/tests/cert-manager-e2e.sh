#!/usr/bin/env bash
# Tests issuance and CA retention on a disposable cluster, with no Grid runtime.
set -Eeuo pipefail
umask 077

CHART=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CLUSTER="${CLUSTER:-grid-enrollment-certmanager-test}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.21.3}"
if [[ "$CERT_MANAGER_VERSION" == v1.21.3 ]]; then
  CERT_MANAGER_SHA256="${CERT_MANAGER_SHA256:-c04a51295adda2b1a775146751f973e787190778c7f35180861d9678876e5c74}"
else
  : "${CERT_MANAGER_SHA256:?set CERT_MANAGER_SHA256 when overriding CERT_MANAGER_VERSION}"
fi
if [[ ! "$CERT_MANAGER_SHA256" =~ ^[a-fA-F0-9]{64}$ ]]; then
  echo "CERT_MANAGER_SHA256 must contain 64 hexadecimal characters" >&2
  exit 1
fi
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

curl -fsSL --retry 3 -o "$WORK/cert-manager.yaml" \
  "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml"
printf '%s  %s\n' "$CERT_MANAGER_SHA256" "$WORK/cert-manager.yaml" | sha256sum --check

if kind get clusters | grep -Fx "$CLUSTER" >/dev/null; then
  echo "Refusing to reuse existing cluster $CLUSTER" >&2
  exit 1
fi
CREATED=true
kind create cluster --name "$CLUSTER" --kubeconfig "$KUBECONFIG" --wait 120s
kubectl apply --server-side -f "$WORK/cert-manager.yaml"
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
  local ca_mode=Existing
  if [[ "$namespace" == fresh ]]; then ca_mode=Generate; fi
  helm upgrade bootstrap "$CHART" -n "$namespace" \
    --set mode=external,external.certManager.enabled=true,external.certManager.issuer.create=true,external.certManager.ca.mode="$ca_mode" \
    --set external.sites.site-a.secretName=site-identity
  kubectl -n "$namespace" wait certificate/site-identity --for=condition=Ready --timeout=120s
  kubectl -n "$namespace" get secret site-identity -o jsonpath='{.data.tls\.crt}' | base64 -d > "$WORK/leaf.crt"
  openssl verify -CAfile "$WORK/ca.crt" -purpose sslclient "$WORK/leaf.crt"
  openssl verify -CAfile "$WORK/ca.crt" -purpose sslserver "$WORK/leaf.crt"
}

helm upgrade --install bootstrap "$CHART" -n fresh --set mode=external,external.certManager.enabled=true,external.certManager.issuer.create=true,external.certManager.ca.mode=Generate
kubectl -n fresh wait certificate/grid-root-ca --for=condition=Ready --timeout=120s
kubectl -n fresh wait issuer/grid-ca --for=condition=Ready --timeout=120s
kubectl -n fresh get secret grid-ca-key -o jsonpath='{.data.tls\.crt}' | base64 -d > "$WORK/ca.crt"
before=$(secret_digest fresh)
issue_leaf fresh

helm upgrade bootstrap "$CHART" -n fresh --reuse-values --set mode=external,external.certManager.enabled=true,external.certManager.issuer.create=true,external.certManager.ca.mode=Generate
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
if [[ -n "${LEGACY_CHART:-}" ]]; then
  helm install bootstrap "$LEGACY_CHART" -n existing --set enabled=true
  issuer_uid=$(kubectl -n existing get issuer grid-ca -o jsonpath='{.metadata.uid}')
fi
helm upgrade --install bootstrap "$CHART" -n existing --set mode=external,external.certManager.enabled=true,external.certManager.issuer.create=true
if [[ -n "${LEGACY_CHART:-}" ]]; then
  [[ "$(kubectl -n existing get issuer grid-ca -o jsonpath='{.metadata.uid}')" == "$issuer_uid" ]]
  [[ "$(secret_digest existing)" == "$existing_before" ]]
  echo "PASS: chart migration preserves the Issuer UID and existing CA"
fi
kubectl -n existing wait issuer/grid-ca --for=condition=Ready --timeout=120s
issue_leaf existing
helm upgrade bootstrap "$CHART" -n existing --reuse-values --set mode=external,external.certManager.enabled=true,external.certManager.issuer.create=true
[[ "$(secret_digest existing)" == "$existing_before" ]]
helm upgrade --install external "$CHART" -n existing \
  --set mode=external,external.certManager.enabled=true,external.certManager.issuer.name=grid-ca \
  --set external.sites.site-b.secretName=external-site-identity
kubectl -n existing wait certificate/external-site-identity --for=condition=Ready --timeout=120s
kubectl -n existing get secret external-site-identity -o jsonpath='{.data.tls\.crt}' | base64 -d > "$WORK/external.crt"
openssl verify -CAfile "$WORK/ca.crt" -purpose sslclient "$WORK/external.crt"
openssl verify -CAfile "$WORK/ca.crt" -purpose sslserver "$WORK/external.crt"
helm uninstall external -n existing
kubectl -n existing get issuer/grid-ca >/dev/null
[[ "$(secret_digest existing)" == "$existing_before" ]]
echo "PASS: external issuer signs site identities and is not owned by the release"

helm uninstall bootstrap -n existing
[[ "$(secret_digest existing)" == "$existing_before" ]]
echo "PASS: existing CA signs leaves and survives upgrades and uninstall unchanged"

helm uninstall bootstrap -n fresh
kubectl -n fresh get certificate/grid-root-ca issuer/grid-root-selfsigned secret/grid-ca-key >/dev/null
[[ "$(secret_digest fresh)" == "$before" ]]
echo "PASS: uninstall retains the generated CA and its bootstrap issuer"
