#!/usr/bin/env bash
# grid-enrollment e2e: prove helm reproducibility AND the real enroll flow on kind.
#
# Deploys the chart, then mints a token, submits a CSR, and asserts a grid-CA-signed
# certificate with the pinned SPIFFE name comes back. Also checks deployment health
# (TLS-only serving, key-separated Secrets, DB verify-full via readiness).
#
# CI: the workflow builds+loads the enrollment image and sets IMAGE_TAG. Local runs
# on rootless podman need `systemd-run --scope --user -p Delegate=yes kind create`
# instead of the plain create below.
#
# Env: IMAGE_REPO (default localhost/grid-enrollment), IMAGE_TAG (default ci),
#      CLUSTER (default grid-enrollment-e2e), KEEP=1 to skip teardown.
set -euo pipefail

IMAGE_REPO="${IMAGE_REPO:-localhost/grid-enrollment}"
IMAGE_TAG="${IMAGE_TAG:-ci}"
CLUSTER="${CLUSTER:-grid-enrollment-e2e}"
CTX="kind-${CLUSTER}"
NS=grid-enroll
PG_IMAGE="quay.io/sclorg/postgresql-16-c9s"
CHART="$(cd "$(dirname "$0")/.." && pwd)/charts/grid-enrollment"
K="kubectl --context ${CTX} -n ${NS}"
WORK="$(mktemp -d)"
# Until this run creates the cluster, the only thing to clean up is the workdir.
# The cluster-teardown trap is armed after create, so a refused run never deletes
# a cluster it does not own.
trap 'rm -rf "${WORK}"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

echo "== cluster + images =="
# Refuse to touch a cluster we do not own: fail on a name collision rather than
# deleting a user's pre-existing cluster. Only a cluster this run creates is torn down.
if kind get clusters 2>/dev/null | grep -qx "${CLUSTER}"; then
  fail "kind cluster '${CLUSTER}' already exists; refusing to clobber it. Delete it or set CLUSTER=<unique-name>."
fi
# KIND_CREATE_PREFIX lets a rootless-podman host pass the systemd Delegate wrapper;
# empty in CI (docker), where plain `kind create` works.
${KIND_CREATE_PREFIX:-} kind create cluster --name "${CLUSTER}"
# We own the cluster now: arm teardown (skipped when KEEP=1).
trap '[ "${KEEP:-0}" = "1" ] || kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true; rm -rf "${WORK}"' EXIT
kind load docker-image "${IMAGE_REPO}:${IMAGE_TAG}" --name "${CLUSTER}"
docker pull "${PG_IMAGE}" 2>/dev/null || podman pull "${PG_IMAGE}"
kind load docker-image "${PG_IMAGE}" --name "${CLUSTER}"

echo "== helm install (authz=local so the e2e can mint with a grid-admin token) =="
helm --kube-context "${CTX}" install grid-enrollment "${CHART}" -n "${NS}" --create-namespace \
  --set "image.repository=${IMAGE_REPO}" --set "image.tag=${IMAGE_TAG}" --set image.pullPolicy=Never \
  --set enrollment.authz=local --timeout 5m
${K} rollout status deploy/grid-enrollment-db --timeout=180s
${K} rollout status deploy/grid-enrollment --timeout=240s

echo "== deployment health =="
${K} get job -l app.kubernetes.io/component=ca-bootstrap -o jsonpath='{.items[0].status.succeeded}' | grep -q 1 \
  || fail "bootstrap Job did not succeed"
pass "bootstrap Job succeeded"
${K} get secret grid-ca-bundle -o jsonpath='{.data.tls\.key}' 2>/dev/null | grep -q . \
  && fail "grid-ca-bundle leaked tls.key (key-separation broken)" || pass "key-separation: bundle has no signing key"
${K} get secret grid-gossip-key >/dev/null 2>&1 && fail "vestigial grid-gossip-key Secret present" \
  || pass "no gossip Secret (identity-only)"

echo "== enroll flow: mint -> CSR -> enroll -> verify =="
ADMIN="$(${K} get secret grid-enrollment-grid-admin-tokens -o jsonpath='{.data.tokens}' | base64 -d | sed 's/^[^:]*://' | tr -d '\n')"
[ -n "${ADMIN}" ] || fail "no grid-admin token generated"
${K} port-forward deploy/grid-enrollment 18443:8443 >"${WORK}/pf.log" 2>&1 &
PF=$!; trap 'kill ${PF} 2>/dev/null || true; [ "${KEEP:-0}" = "1" ] || kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true; rm -rf "${WORK}"' EXIT
sleep 4

# plaintext must be refused (TLS-only listener)
code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:18443/readyz" || true)"
[ "${code}" = "000" ] && pass "plaintext refused (TLS-only)" || fail "plaintext got HTTP ${code}, expected refusal"
# readiness over TLS
curl -sk -o /dev/null -w '%{http_code}' "https://127.0.0.1:18443/readyz" | grep -q 200 \
  || fail "/readyz not 200 over TLS"; pass "/readyz 200 over TLS"

MINT="$(curl -sk -X POST "https://127.0.0.1:18443/v1alpha1/enrollmenttokens" \
  -H "Authorization: Bearer ${ADMIN}" -H "Content-Type: application/json" \
  -d '{"siteName":"site-a","gridNetworkRef":"grid-1"}')"
TOKEN="$(printf '%s' "${MINT}" | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")" \
  || fail "mint failed: ${MINT}"
pass "minted site token pinning site-a"

openssl ecparam -name prime256v1 -genkey -noout -out "${WORK}/site.key" 2>/dev/null
openssl req -new -key "${WORK}/site.key" -subj "/CN=site-a" -out "${WORK}/site.csr" 2>/dev/null
python3 -c "import json;open('${WORK}/body.json','w').write(json.dumps({'csr':open('${WORK}/site.csr').read()}))"

ENROLL="$(curl -sk -X POST "https://127.0.0.1:18443/v1alpha1/enrollments" \
  -H "Authorization: Bearer ${TOKEN}" -H "Content-Type: application/json" \
  --data @"${WORK}/body.json")"
printf '%s' "${ENROLL}" | python3 -c "import sys,json;d=json.load(sys.stdin);open('${WORK}/leaf.pem','w').write(d['certificate']);open('${WORK}/ca.pem','w').write(d['caCertificate']);print(d['spiffeId'])" >"${WORK}/spiffe" \
  || fail "enroll failed: ${ENROLL}"
SPIFFE="$(cat "${WORK}/spiffe")"

openssl verify -CAfile "${WORK}/ca.pem" "${WORK}/leaf.pem" >/dev/null 2>&1 \
  || fail "issued leaf does not chain to the returned grid CA"
pass "leaf chains to the grid CA"
[ "${SPIFFE}" = "spiffe://grid.internal/site/site-a" ] \
  || fail "spiffeId ${SPIFFE} is not the pinned name"
pass "spiffeId pins the grid-assigned name: ${SPIFFE}"
openssl x509 -in "${WORK}/leaf.pem" -noout -ext subjectAltName 2>/dev/null | grep -q "spiffe://grid.internal/site/site-a" \
  || fail "leaf SAN missing the pinned SPIFFE URI"
pass "leaf SAN carries the pinned SPIFFE URI"

echo "== e2e GREEN: helm reproducible + real enroll flow verified =="
