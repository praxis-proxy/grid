#!/usr/bin/env bash
# The ACM path end to end on kind: two clusters, cert-manager identities, Local membership,
# no enrollment service anywhere.
#
# Each cluster is a site. Its operator's config carries a site name and nothing else, its
# identity comes from cert-manager through a CA both clusters trust, and its members are the
# GridSites its own configuration declares. Neither site enrolls, neither runs SWIM, and
# nothing installs grid-enrollment. What is under test is whether two sites built that way
# poll each other's signals.
#
# One cluster per site, because the operator reads every GridNetwork it can see and a second
# site in another namespace would give it two grids to serve.
#
# This branch carries the Local membership half. The other half, identity.source CertManager
# on the grid-operator chart, is on feat/site-identity-certmanager, so the leg runs only where
# both are present. It refuses to start otherwise rather than failing in a helm schema error.
#
# Env: IMAGE_REPO (default localhost/grid-operator), IMAGE_TAG (default ci),
#      PREFIX (default grid-acm), GRID_ID (default dagobah),
#      CERT_MANAGER_VERSION, KEEP=1 to keep the clusters, KIND_CREATE_PREFIX for rootless podman.
set -euo pipefail

IMAGE_REPO="${IMAGE_REPO:-localhost/grid-operator}"
IMAGE_TAG="${IMAGE_TAG:-ci}"
PREFIX="${PREFIX:-grid-acm}"
GRID_ID="${GRID_ID:-dagobah}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
NS=grid
NETWORK=grid
SITES=(east west)
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WORK="$(mktemp -d)"
FAILED=0
CREATED=()
declare -A ENDPOINT
trap 'rm -rf "${WORK}"' EXIT

ctx() { printf 'kind-%s-%s' "${PREFIX}" "$1"; }
k() { local site=$1; shift; kubectl --context "$(ctx "${site}")" "$@"; }
ns() { local site=$1; shift; kubectl --context "$(ctx "${site}")" -n "${NS}" "$@"; }
fail() { echo "FAIL: $*" >&2; FAILED=1; }
pass() { echo "PASS: $*"; }
note() { echo "NOTE: $*"; }
die() { echo "FATAL: $*" >&2; exit 1; }

teardown() {
  [[ ${KEEP:-0} == 1 ]] && return 0
  local site
  for site in "${CREATED[@]+"${CREATED[@]}"}"; do
    kind delete cluster --name "${PREFIX}-${site}" >/dev/null 2>&1 || true
  done
}

eventually() {
  local limit=$1 what=$2 deadline
  shift 2
  deadline=$(( $(date -u +%s) + limit ))
  until "$@" >/dev/null 2>&1; do
    if (( $(date -u +%s) >= deadline )); then
      fail "${what}: still not true after ${limit}s"
      return 1
    fi
    sleep 3
  done
  pass "${what}"
}

# condition <site> <object> <type> <field>
condition() {
  local out
  out=$(ns "$1" get "$2" -o json 2>/dev/null \
    | jq -r ".status.conditions[]? | select(.type==\"$3\") | .$4" 2>/dev/null) || return 1
  [[ -n ${out} && ${out} != null ]] || return 1
  printf '%s' "${out}"
}

# own_condition <site> <type> <field>: this site's own conditions, wherever they live.
own_condition() {
  condition "$1" "gridnetwork/${NETWORK}" "$2" "$3" || condition "$1" "gridsite/$1" "$2" "$3"
}

# Refuse early and by name: without the cert-manager chart support, the install below fails
# with "additional properties 'identity' not allowed", which says nothing about why.
grep -q '"identity"' "${ROOT}/charts/grid-operator/values.schema.json" \
  || die "this checkout has no identity.source: merge feat/site-identity-certmanager, which carries the cert-manager chart support this leg needs"

echo "== clusters =="
clusters=$(kind get clusters 2>/dev/null || true)
for site in "${SITES[@]}"; do
  grep -qx "${PREFIX}-${site}" <<<"${clusters}" && die "kind cluster '${PREFIX}-${site}' already exists"
done
for site in "${SITES[@]}"; do
  ${KIND_CREATE_PREFIX:-} kind create cluster --name "${PREFIX}-${site}" || die "cannot create ${site}"
  CREATED+=("${site}")
  trap 'teardown; rm -rf "${WORK}"' EXIT
  kind load docker-image "${IMAGE_REPO}:${IMAGE_TAG}" --name "${PREFIX}-${site}" || die "cannot load the image into ${site}"
  k "${site}" create namespace "${NS}"
done

echo "== cert-manager in each cluster =="
for site in "${SITES[@]}"; do
  k "${site}" apply -f \
    "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml" \
    || die "cannot install cert-manager in ${site}"
done
for site in "${SITES[@]}"; do
  for d in cert-manager cert-manager-webhook cert-manager-cainjector; do
    k "${site}" -n cert-manager rollout status "deployment/${d}" --timeout=300s \
      || die "${site}: cert-manager ${d} not ready"
  done
done

echo "== one CA, an Issuer in each cluster =="
# One CA for the grid, held by the platform team in the real shape. Each site gets the key
# only because this stands in for a CA neither cluster runs; a Vault issuer would hold it.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
  -subj "/CN=grid-ca/O=ai-grid" -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "${WORK}/ca.key" -out "${WORK}/ca.crt" 2>/dev/null || die "cannot make the grid CA"
cat > "${WORK}/issuer.yaml" <<EOF
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: grid-site
spec:
  ca:
    secretName: grid-ca-keypair
EOF
for site in "${SITES[@]}"; do
  ns "${site}" create secret tls grid-ca-keypair --cert="${WORK}/ca.crt" --key="${WORK}/ca.key"
  ns "${site}" create secret generic grid-ca --from-file=ca.crt="${WORK}/ca.crt"
  eventually 180 "${site}: cert-manager accepts an Issuer" ns "${site}" apply -f "${WORK}/issuer.yaml" \
    || die "${site}: cert-manager never accepted the Issuer"
done

echo "== an operator per site, enrolling nothing =="
for site in "${SITES[@]}"; do
  helm --kube-context "$(ctx "${site}")" install grid-operator "${ROOT}/charts/grid-operator" -n "${NS}" \
    --set "image.repository=${IMAGE_REPO}" --set "image.tag=${IMAGE_TAG}" --set image.pullPolicy=Never \
    --set "site.name=${site}" --set grid.peerTrust=spiffe --set grid.membership.source=Local \
    --set identity.source=CertManager --set identity.issuerRef.name=grid-site \
    --set signals.enabled=true --set signals.service.type=NodePort \
    || die "cannot install ${site}'s operator"
done

echo "== where each site answers =="
# A NodePort on the kind node, which the other cluster's containers reach on the same network.
# A real deployment publishes a Route or a load balancer instead.
for site in "${SITES[@]}"; do
  ip=$(k "${site}" get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}') \
    || die "cannot read ${site}'s node address"
  port=$(ns "${site}" get svc grid-operator-signals -o jsonpath='{.spec.ports[0].nodePort}') \
    || die "cannot read ${site}'s signals NodePort"
  [[ -n ${ip} && -n ${port} ]] || die "${site} has no reachable signals address"
  ENDPOINT[${site}]="${ip}:${port}"
  note "${site} serves signals at ${ENDPOINT[${site}]}"
done

echo "== each site declares the grid and both members =="
# Under ACM the hub delivers these, identical on every cluster.
for site in "${SITES[@]}"; do
  {
    cat <<EOF
apiVersion: grid.praxis.fast/v1alpha1
kind: GridNetwork
metadata:
  name: ${NETWORK}
spec:
  gridId: "${GRID_ID}"
  peerTrust:
    mode: spiffe
  signalTransport:
    mode: poll
  tls:
    siteSecretRef:
      name: grid-site-identity
      namespace: ${NS}
    caSecretRef:
      name: grid-ca
      namespace: ${NS}
EOF
    for member in "${SITES[@]}"; do
      echo "---"
      cat <<EOF
apiVersion: grid.praxis.fast/v1alpha1
kind: GridSite
metadata:
  name: ${member}
spec:
  gridNetworkRef: ${NETWORK}
EOF
      [[ ${member} == "${site}" ]] && continue
      cat <<EOF
  signalsEndpoint:
    address: ${ENDPOINT[${member}]}
    serverName: ${member}-signals.grid.internal
EOF
    done
  } > "${WORK}/${site}-grid.yaml"
  eventually 120 "${site}: the grid and both members are declared" \
    ns "${site}" apply --server-side --force-conflicts --field-manager=e2e-acm -f "${WORK}/${site}-grid.yaml" \
    || die "cannot declare ${site}'s grid"
done

echo "== identities =="
for site in "${SITES[@]}"; do
  eventually 180 "${site}: cert-manager wrote the identity" ns "${site}" get secret grid-site-identity \
    || die "${site} never got an identity"
  ns "${site}" rollout status deployment/grid-operator --timeout=300s || die "${site}'s operator never came up"
  eventually 240 "${site}: IdentityValid is reported" own_condition "${site}" IdentityValid status
  got=$(own_condition "${site}" IdentityValid status || echo missing)
  [[ ${got} == True ]] && pass "${site}: IdentityValid is True" \
    || fail "${site}: IdentityValid is ${got}: $(own_condition "${site}" IdentityValid message || true)"
done

echo "== no enrollment anywhere =="
# The claim the mode rests on: nothing enrolls, no signer is named, and SWIM does not run.
for site in "${SITES[@]}"; do
  if k "${site}" get deployments -A -o name 2>/dev/null | grep -q grid-enrollment; then
    fail "${site}: a grid-enrollment Deployment is installed"
  else
    pass "${site}: no grid-enrollment Deployment in the cluster"
  fi
  config=$(ns "${site}" get configmap grid-operator-config -o jsonpath='{.data.config\.yaml}')
  grep -q signerEndpoint <<<"${config}" && fail "${site}: the operator config names a signer" \
    || pass "${site}: the operator config names no signer"
  if ns "${site}" get svc grid-operator-swim >/dev/null 2>&1; then
    fail "${site}: a SWIM Service renders under Local membership"
  else
    pass "${site}: no SWIM Service under Local membership"
  fi
done

echo "== each site polls the other =="
for site in "${SITES[@]}"; do
  for member in "${SITES[@]}"; do
    [[ ${member} == "${site}" ]] && continue
    eventually 300 "${site}: SignalsReachable is reported for ${member}" \
      condition "${site}" "gridsite/${member}" SignalsReachable status
    reach=$(condition "${site}" "gridsite/${member}" SignalsReachable status || echo missing)
    [[ ${reach} == True ]] && pass "${site} reaches ${member}'s signals" \
      || fail "${site}: SignalsReachable for ${member} is ${reach}: $(condition "${site}" "gridsite/${member}" SignalsReachable message || true)"
    accepted=$(condition "${site}" "gridsite/${member}" CatalogAccepted status || echo missing)
    note "${site}: CatalogAccepted for ${member} is ${accepted}: $(condition "${site}" "gridsite/${member}" CatalogAccepted message || true)"
  done
done

if (( FAILED )); then
  echo "RESULT: FAIL"
  exit 1
fi
echo "RESULT: PASS"
