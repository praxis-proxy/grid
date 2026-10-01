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
#      CLUSTER (default grid-enrollment-e2e), KEEP=1 to skip teardown,
#      AUTHZ=local|kube (default local; kube mints with an audience-bound SA token).
set -euo pipefail

IMAGE_REPO="${IMAGE_REPO:-localhost/grid-enrollment}"
IMAGE_TAG="${IMAGE_TAG:-ci}"
CLUSTER="${CLUSTER:-grid-enrollment-e2e}"
AUTHZ="${AUTHZ:-local}"
AUDIENCE=grid-enrollment
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

# Under kube the chart ships the grid-admin Role, SA and binding; grid-subj is
# bound through gridAdmins.subjects.
KUBE_ARGS=()
if [ "${AUTHZ}" = "kube" ]; then
  KUBE_ARGS=(--set enrollment.gridAdmins.serviceAccount.create=true
    --set enrollment.gridAdmins.subjects[0].kind=ServiceAccount
    --set enrollment.gridAdmins.subjects[0].name=grid-subj
    --set "enrollment.gridAdmins.subjects[0].namespace=${NS}")
fi
if [ "${AUTHZ}" = "kube" ]; then
  # A cluster-wide group as a grid-admin subject must fail the render.
  if helm template grid-enrollment "${CHART}" -n "${NS}" --set enrollment.authz=kube \
      --set enrollment.gridAdmins.subjects[0].kind=Group \
      --set enrollment.gridAdmins.subjects[0].name=system:authenticated >/dev/null 2>"${WORK}/render.err"; then
    fail "chart rendered system:authenticated as a grid-admin subject"
  fi
  # Schema or template guard; either names the subjects field.
  grep -qE "gridAdmins[/.]subjects" "${WORK}/render.err" || fail "render failed for another reason: $(cat "${WORK}/render.err")"
  pass "system:authenticated grid-admin subject fails the render"
fi
echo "== helm install (authz=${AUTHZ}) =="
helm --kube-context "${CTX}" install grid-enrollment "${CHART}" -n "${NS}" --create-namespace \
  --set "image.repository=${IMAGE_REPO}" --set "image.tag=${IMAGE_TAG}" --set image.pullPolicy=Never \
  --set "enrollment.authz=${AUTHZ}" --set "enrollment.tokenAudience=${AUDIENCE}" "${KUBE_ARGS[@]}" --timeout 5m
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
if [ "${AUTHZ}" = "kube" ]; then
  # grid-admin and grid-subj: the chart's Role. grid-cadmin: a ClusterRole.
  # grid-otherns: a Role in another namespace. grid-noperm: no grant.
  for sa in grid-subj grid-cadmin grid-otherns grid-noperm; do ${K} create serviceaccount "${sa}"; done
  # Applied as YAML: `kubectl create role --resource` needs discovery, and
  # enrollmenttokens has no CRD (only the SubjectAccessReview sees it).
  kubectl --context "${CTX}" create namespace grid-other
  rbac() { # <kind> <namespace or ""> <binding sa>
    local ns_meta="" role_kind="$1" bind_kind="$1Binding"
    [ -n "$2" ] && ns_meta="namespace: $2"
    kubectl --context "${CTX}" apply -f - <<YAML
apiVersion: rbac.authorization.k8s.io/v1
kind: ${role_kind}
metadata: {name: grid-enrollment-admin${ns_meta:+, ${ns_meta}}}
rules:
  - apiGroups: [grid.praxis-proxy.io]
    resources: [enrollmenttokens]
    verbs: [create, delete]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: ${bind_kind}
metadata: {name: grid-enrollment-admin${ns_meta:+, ${ns_meta}}}
roleRef: {apiGroup: rbac.authorization.k8s.io, kind: ${role_kind}, name: grid-enrollment-admin}
subjects: [{kind: ServiceAccount, name: $3, namespace: ${NS}}]
YAML
  }
  rbac Role grid-other grid-otherns
  rbac ClusterRole "" grid-cadmin
  sa_token() { ${K} create token "$1" --duration 10m "${@:2}"; }
  ADMIN="$(sa_token grid-admin --audience "${AUDIENCE}")"
  SUBJ="$(sa_token grid-subj --audience "${AUDIENCE}")"
  CADMIN="$(sa_token grid-cadmin --audience "${AUDIENCE}")"
  OTHER_NS="$(sa_token grid-otherns --audience "${AUDIENCE}")"
  NO_PERM="$(sa_token grid-noperm --audience "${AUDIENCE}")"
  NO_AUD="$(sa_token grid-admin)"
else
  ADMIN="$(${K} get secret grid-enrollment-grid-admin-tokens -o jsonpath='{.data.tokens}' | base64 -d | sed 's/^[^:]*://' | tr -d '\n')"
fi
[ -n "${ADMIN}" ] || fail "no grid-admin token"
${K} port-forward deploy/grid-enrollment 18443:8443 >"${WORK}/pf.log" 2>&1 &
PF=$!; trap 'kill ${PF} 2>/dev/null || true; [ "${KEEP:-0}" = "1" ] || kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true; rm -rf "${WORK}"' EXIT
sleep 4

# plaintext must be refused (TLS-only listener)
code="$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 "http://127.0.0.1:18443/readyz" || true)"
[ "${code}" = "000" ] && pass "plaintext refused (TLS-only)" || fail "plaintext got HTTP ${code}, expected refusal"
# readiness over TLS
curl -sk -o /dev/null -w '%{http_code}' "https://127.0.0.1:18443/readyz" | grep -q 200 \
  || fail "/readyz not 200 over TLS"; pass "/readyz 200 over TLS"

# mint <bearer> [site]: writes the body to mint.json, prints the HTTP status.
mint() {
  curl -sk -o "${WORK}/mint.json" -w '%{http_code}' -X POST "https://127.0.0.1:18443/v1alpha1/enrollmenttokens" \
    -H "Authorization: Bearer $1" -H "Content-Type: application/json" \
    -d "{\"siteName\":\"${2:-site-a}\",\"gridNetworkRef\":\"grid-1\"}"
}
expect_mint() { # <want> <bearer> <site> <what>
  local code; code="$(mint "$2" "$3")"
  [ "${code}" = "$1" ] || fail "$4 got HTTP ${code}, expected $1: $(cat "${WORK}/mint.json")"
  pass "$4 ($1)"
}
if [ "${AUTHZ}" = "kube" ]; then
  expect_mint 401 "${NO_AUD}" site-x "token without the ${AUDIENCE} audience refused"
  expect_mint 403 "${NO_PERM}" site-x "audience-bound token without RBAC refused"
  expect_mint 403 "${OTHER_NS}" site-x "Role in another namespace refused"
  expect_mint 201 "${SUBJ}" site-s "chart gridAdmins.subjects grant mints"
  expect_mint 201 "${CADMIN}" site-c "ClusterRole grant mints"
fi
code="$(mint "${ADMIN}")"
[ "${code}" = "201" ] || fail "mint got HTTP ${code}: $(cat "${WORK}/mint.json")"
MINT="$(cat "${WORK}/mint.json")"
TOKEN="$(printf '%s' "${MINT}" | python3 -c "import sys,json;print(json.load(sys.stdin)['token'])")" \
  || fail "mint failed: ${MINT}"
pass "minted site token pinning site-a (authz=${AUTHZ}, 201)"

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

echo "== e2e GREEN (authz=${AUTHZ}): helm reproducible + real enroll flow verified =="
