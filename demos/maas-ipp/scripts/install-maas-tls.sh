#!/usr/bin/env bash
# Create cert-manager CA chain + maas-api serving certificate (Kind TLS path).
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_cmd kubectl

kc create namespace "$MAAS_NAMESPACE" --dry-run=client -o yaml | kc apply -f -

if kc get secret maas-api-serving-cert -n "$MAAS_NAMESPACE" &>/dev/null; then
  ok "maas-api TLS certificate already exists"
  exit 0
fi

# Wait for cert-manager webhook to accept ClusterIssuers
_retries=0
while [[ $_retries -lt 24 ]]; do
  if kc apply --dry-run=server -f - <<'CMEOF' 2>/dev/null
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: cert-manager-webhook-probe
spec:
  selfSigned: {}
CMEOF
  then
    break
  fi
  sleep 5
  _retries=$((_retries + 1))
done

kc apply -f - <<EOF
apiVersion: cert-manager.io/v1
kind: ClusterIssuer
metadata:
  name: maas-selfsigned-issuer
spec:
  selfSigned: {}
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: maas-root-ca
  namespace: ${MAAS_NAMESPACE}
spec:
  isCA: true
  commonName: maas-ca
  secretName: maas-root-ca
  issuerRef:
    name: maas-selfsigned-issuer
    kind: ClusterIssuer
  duration: 87600h
---
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: maas-ca-issuer
  namespace: ${MAAS_NAMESPACE}
spec:
  ca:
    secretName: maas-root-ca
---
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: maas-api-serving-cert
  namespace: ${MAAS_NAMESPACE}
spec:
  secretName: maas-api-serving-cert
  issuerRef:
    name: maas-ca-issuer
    kind: Issuer
  dnsNames:
  - maas-api
  - maas-api.${MAAS_NAMESPACE}
  - maas-api.${MAAS_NAMESPACE}.svc
  - maas-api.${MAAS_NAMESPACE}.svc.cluster.local
  duration: 8760h
  renewBefore: 720h
EOF

kc wait --for=condition=Ready certificate/maas-api-serving-cert \
  -n "$MAAS_NAMESPACE" --timeout=120s
ok "maas-api TLS certificate ready"
