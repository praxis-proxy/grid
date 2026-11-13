#!/usr/bin/env bash
# Delivered membership records end to end on kind: two clusters, Local membership, no
# enrollment service anywhere.
#
# Each cluster is a site. Its operator runs without an operator config, and its
# members are the GridSites its own configuration declares, applied here the way an external
# source of truth delivers them: the same records on every cluster, written by something
# outside the grid. Open Cluster Management is the reference example; this leg applies the
# records directly rather than running it, so what is under test is the grid's side of that
# contract and not the delivery mechanism. Neither site enrolls, neither runs SWIM, and
# nothing installs grid-enrollment.
#
# Models are then added and removed on a site, and a third site joins the running grid and is
# removed again. What a peer reads from a site is checked as an exact list after each change.
# Members change by records alone: no site learns of a member, or of its removal, from a peer.
#
# One cluster per site, because the operator reads every GridNetwork it can see and a second
# site in another namespace would give it two grids to serve.
#
# IDENTITY says where each site's identity comes from. secrets, the default, signs the two
# leaves here with one CA and writes them, which needs nothing outside this branch.
# certmanager installs cert-manager and has it issue them, which needs
# identity.source CertManager on the grid-operator chart; the leg refuses by name when that
# is absent rather than failing in a helm schema error.
#
# Env: IMAGE_REPO (default localhost/grid-operator), IMAGE_TAG (default ci),
#      PREFIX (default grid-acm), GRID_ID (default dagobah), IDENTITY (secrets or certmanager),
#      CERT_MANAGER_VERSION, KEEP=1 to keep the clusters, KIND_CREATE_PREFIX for rootless podman.
set -euo pipefail

IMAGE_REPO="${IMAGE_REPO:-localhost/grid-operator}"
IMAGE_TAG="${IMAGE_TAG:-ci}"
PREFIX="${PREFIX:-grid-acm}"
GRID_ID="${GRID_ID:-dagobah}"
CERT_MANAGER_VERSION="${CERT_MANAGER_VERSION:-v1.16.2}"
IDENTITY="${IDENTITY:-secrets}"
NS=grid
NETWORK=grid
SITES=(east west)
JOINER=north
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

case ${IDENTITY} in
  secrets) ;;
  certmanager)
    # Refuse early and by name: without the cert-manager chart support, the install below
    # fails with "additional properties 'identity' not allowed", which says nothing about why.
    grep -q '"identity"' "${ROOT}/charts/grid-operator/values.schema.json" \
      || die "IDENTITY=certmanager needs identity.source, which feat/site-identity-certmanager carries; this checkout has no such value"
    ;;
  *) die "IDENTITY must be secrets or certmanager, not ${IDENTITY}" ;;
esac

echo "== clusters =="
clusters=$(kind get clusters 2>/dev/null || true)
for site in "${SITES[@]}" "${JOINER}"; do
  grep -qx "${PREFIX}-${site}" <<<"${clusters}" && die "kind cluster '${PREFIX}-${site}' already exists"
done

# create_clusters <site>...
create_clusters() {
  local site
  for site in "$@"; do
    ${KIND_CREATE_PREFIX:-} kind create cluster --name "${PREFIX}-${site}" || die "cannot create ${site}"
    CREATED+=("${site}")
    trap 'teardown; rm -rf "${WORK}"' EXIT
    kind load docker-image "${IMAGE_REPO}:${IMAGE_TAG}" --name "${PREFIX}-${site}" || die "cannot load the image into ${site}"
    k "${site}" create namespace "${NS}"
  done
}

# install_cert_manager <site>...
install_cert_manager() {
  local site d
  for site in "$@"; do
    k "${site}" apply -f \
      "https://github.com/cert-manager/cert-manager/releases/download/${CERT_MANAGER_VERSION}/cert-manager.yaml" \
      || die "cannot install cert-manager in ${site}"
  done
  for site in "$@"; do
    for d in cert-manager cert-manager-webhook cert-manager-cainjector; do
      k "${site}" -n cert-manager rollout status "deployment/${d}" --timeout=300s \
        || die "${site}: cert-manager ${d} not ready"
    done
  done
}

create_clusters "${SITES[@]}"
if [[ ${IDENTITY} == certmanager ]]; then
  echo "== cert-manager in each cluster =="
  install_cert_manager "${SITES[@]}"
fi

echo "== one CA for the grid =="
# One CA, held by the platform team in the real shape. The leg holds the key only because it
# stands in for a CA neither cluster runs; a Vault issuer would hold it.
openssl req -x509 -newkey ec -pkeyopt ec_paramgen_curve:prime256v1 -nodes -days 30 \
  -subj "/CN=grid-ca/O=ai-grid" -addext "basicConstraints=critical,CA:TRUE" \
  -addext "keyUsage=critical,keyCertSign,cRLSign" \
  -keyout "${WORK}/ca.key" -out "${WORK}/ca.crt" 2>/dev/null || die "cannot make the grid CA"

# site_leaf <site>: a leaf for <site> with the names a peer verifies, signed by that CA.
site_leaf() {
  local site=$1
  openssl ecparam -name prime256v1 -genkey -noout -out "${WORK}/${site}.key" 2>/dev/null
  openssl req -new -key "${WORK}/${site}.key" -subj "/CN=${site}/O=${site}" -out "${WORK}/${site}.csr" 2>/dev/null
  cat > "${WORK}/${site}.ext" <<EOF
basicConstraints=critical,CA:FALSE
keyUsage=critical,digitalSignature
extendedKeyUsage=clientAuth,serverAuth
subjectAltName=DNS:${site}.grid.internal,URI:spiffe://grid.internal/site/${site}
EOF
  openssl x509 -req -in "${WORK}/${site}.csr" -CA "${WORK}/ca.crt" -CAkey "${WORK}/ca.key" \
    -extfile "${WORK}/${site}.ext" -days 7 -out "${WORK}/${site}.crt" 2>/dev/null
}

cat > "${WORK}/issuer.yaml" <<EOF
apiVersion: cert-manager.io/v1
kind: Issuer
metadata:
  name: grid-site
spec:
  ca:
    secretName: grid-ca-keypair
EOF
# give_identities <site>...
give_identities() {
  local site
  for site in "$@"; do
    ns "${site}" create secret generic grid-ca --from-file=ca.crt="${WORK}/ca.crt"
    if [[ ${IDENTITY} == certmanager ]]; then
      ns "${site}" create secret tls grid-ca-keypair --cert="${WORK}/ca.crt" --key="${WORK}/ca.key"
      eventually 180 "${site}: cert-manager accepts an Issuer" ns "${site}" apply -f "${WORK}/issuer.yaml" \
        || die "${site}: cert-manager never accepted the Issuer"
    else
      # The identity a delivered-records install gets from whatever its CA team runs. The leaf
      # carries the names a peer verifies: the SPIFFE URI, the DNS name and the organization.
      site_leaf "${site}" || die "cannot sign ${site}'s leaf"
      ns "${site}" create secret tls grid-site-identity \
        --cert="${WORK}/${site}.crt" --key="${WORK}/${site}.key" >/dev/null \
        || die "cannot write ${site}'s identity"
      pass "${site}: holds an identity from the grid CA"
    fi
  done
}
give_identities "${SITES[@]}"

echo "== an operator per site, enrolling nothing =="
CERTMANAGER_ARGS=()
if [[ ${IDENTITY} == certmanager ]]; then
  CERTMANAGER_ARGS=(--set identity.source=CertManager --set identity.issuerRef.name=grid-site)
fi
# install_operators <site>...
install_operators() {
  local site
  for site in "$@"; do
    helm --kube-context "$(ctx "${site}")" install grid-operator "${ROOT}/charts/grid-operator" -n "${NS}" \
      --set "image.repository=${IMAGE_REPO}" --set "image.tag=${IMAGE_TAG}" --set image.pullPolicy=Never \
      --set "site.name=${site}" --set grid.peerTrust=spiffe --set grid.membership.source=Local \
      ${CERTMANAGER_ARGS[@]+"${CERTMANAGER_ARGS[@]}"} \
      --set signals.enabled=true --set signals.service.type=NodePort \
      || die "cannot install ${site}'s operator"
  done
}
install_operators "${SITES[@]}"

echo "== where each site answers =="
# A NodePort on the kind node, which the other cluster's containers reach on the same network.
# A real deployment publishes a Route or a load balancer instead.
# read_endpoints <site>...
read_endpoints() {
  local site ip port
  for site in "$@"; do
    ip=$(k "${site}" get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}') \
      || die "cannot read ${site}'s node address"
    port=$(ns "${site}" get svc grid-operator-signals -o jsonpath='{.spec.ports[0].nodePort}') \
      || die "cannot read ${site}'s signals NodePort"
    [[ -n ${ip} && -n ${port} ]] || die "${site} has no reachable signals address"
    ENDPOINT[${site}]="${ip}:${port}"
    note "${site} serves signals at ${ENDPOINT[${site}]}"
  done
}
read_endpoints "${SITES[@]}"

echo "== each site declares the grid and both members =="
# Under ACM the hub delivers these, identical on every cluster.
# deliver <site> <member>...: the grid and those members, written to <site>.
deliver() {
  local site=$1 member
  shift
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
    for member in "$@"; do
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
  eventually 120 "${site}: the grid and $# members are declared" \
    ns "${site}" apply --server-side --force-conflicts --field-manager=e2e-acm -f "${WORK}/${site}-grid.yaml" \
    || die "cannot declare ${site}'s grid"
}
for site in "${SITES[@]}"; do
  deliver "${site}" "${SITES[@]}"
done

echo "== identities =="
# assert_identities <site>...
assert_identities() {
  local site got
  for site in "$@"; do
    eventually 180 "${site}: the identity Secret is in place" ns "${site}" get secret grid-site-identity \
      || die "${site} never got an identity"
    ns "${site}" rollout status deployment/grid-operator --timeout=300s || die "${site}'s operator never came up"
    eventually 240 "${site}: IdentityValid is reported" own_condition "${site}" IdentityValid status
    got=$(own_condition "${site}" IdentityValid status || echo missing)
    [[ ${got} == True ]] && pass "${site}: IdentityValid is True" \
      || fail "${site}: IdentityValid is ${got}: $(own_condition "${site}" IdentityValid message || true)"
  done
}
assert_identities "${SITES[@]}"

echo "== no enrollment anywhere =="
# The claim the mode rests on: nothing enrolls, no signer is named, and SWIM does not run.
# assert_no_enrollment <site>...
assert_no_enrollment() {
  local site config deployments swim
  for site in "$@"; do
    # A failed query proves nothing absent, so it fails the check and keeps kubectl's error.
    if ! deployments=$(k "${site}" get deployments -A -o name); then
      fail "${site}: cannot list Deployments to look for grid-enrollment"
    elif grep -q grid-enrollment <<<"${deployments}"; then
      fail "${site}: a grid-enrollment Deployment is installed"
    else
      pass "${site}: no grid-enrollment Deployment in the cluster"
    fi
    # Without enrollment values the chart renders no operator config, which names no signer either.
    if ! config=$(ns "${site}" get configmap grid-operator-config --ignore-not-found -o jsonpath='{.data.config\.yaml}'); then
      fail "${site}: cannot read the operator config"
    elif grep -q signerEndpoint <<<"${config}"; then
      fail "${site}: the operator config names a signer"
    else
      pass "${site}: the operator config names no signer"
    fi
    if ! swim=$(ns "${site}" get svc grid-operator-swim --ignore-not-found -o name); then
      fail "${site}: cannot read the SWIM Service"
    elif [[ -n ${swim} ]]; then
      fail "${site}: a SWIM Service renders under Local membership"
    else
      pass "${site}: no SWIM Service under Local membership"
    fi
  done
}
assert_no_enrollment "${SITES[@]}"

echo "== each site polls the other =="
# reaches <site> <member>: <site>'s last poll of <member> answered.
reaches() { [[ $(condition "$1" "gridsite/$2" SignalsReachable status) == True ]]; }
# assert_polls <site> <member>
assert_polls() {
  local site=$1 member=$2 accepted
  # Waits for True: a site refused before its record was delivered has already reported False.
  eventually 300 "${site} reaches ${member}'s signals" reaches "${site}" "${member}" \
    || note "${site}: SignalsReachable for ${member}: $(condition "${site}" "gridsite/${member}" SignalsReachable message || true)"
  accepted=$(condition "${site}" "gridsite/${member}" CatalogAccepted status || echo missing)
  note "${site}: CatalogAccepted for ${member} is ${accepted}: $(condition "${site}" "gridsite/${member}" CatalogAccepted message || true)"
}
for site in "${SITES[@]}"; do
  for member in "${SITES[@]}"; do
    [[ ${member} == "${site}" ]] || assert_polls "${site}" "${member}"
  done
done

echo "== what a peer reads from a site =="
# A site answers its declared peers with its catalog, and that is all that crosses between
# sites: who the members are never does. The reads below are made the way a peer's operator
# makes them, from the reader's own cluster with the reader's own identity.
ENGINE="${CONTAINER_ENGINE:-$(command -v docker || command -v podman || true)}"
[[ -n ${ENGINE} ]] || die "no container engine to reach the kind nodes with"
node() { "${ENGINE}" exec -i "${PREFIX}-$1-control-plane" "${@:2}"; }

# hold_identity <site>...: each site's node holds that site's leaf, to read a peer with.
hold_identity() {
  local site file
  for site in "$@"; do
    for file in tls.crt tls.key; do
      ns "${site}" get secret grid-site-identity -o json | jq -r --arg f "${file}" '.data[$f] | @base64d' \
        | node "${site}" sh -c "umask 077; cat > /tmp/reader-${file}" || die "${site}: cannot hold its identity"
    done
    node "${site}" sh -c 'cat > /tmp/reader-ca.crt' < "${WORK}/ca.crt" || die "${site}: cannot hold the grid CA"
  done
}

# read_catalog <reader> <member>: the status <member> answers <reader> with, then one
# "provider model" line per model it offers, sorted. A failed request returns curl's exit
# status, and a 200 whose body is not a providers catalog returns 97.
read_catalog() {
  local reader=$1 member=$2 port=${ENDPOINT[$2]##*:} body status models
  body=$(node "${reader}" curl -sS -m 10 --cacert /tmp/reader-ca.crt \
    --cert /tmp/reader-tls.crt --key /tmp/reader-tls.key \
    --resolve "${member}.grid.internal:${port}:${ENDPOINT[${member}]%%:*}" \
    -w '\n%{http_code}' "https://${member}.grid.internal:${port}/v1/site/providers") || return
  status=${body##*$'\n'}
  echo "${status}"
  [[ ${status} == 200 ]] || return 0
  models=$(jq -r 'if (.providers | type) == "array"
    then .providers[] | .providerId as $p | .models[] | "\($p) \(.)"
    else error("no providers array") end' <<<"${body%$'\n'*}") || return 97
  [[ -z ${models} ]] || sort <<<"${models}"
}

# reads <reader> <member> <status> [<provider model>...]: <member> answers <reader> with
# exactly this status and exactly these models, no more and no fewer.
reads() {
  local reader=$1 member=$2 want got
  want=$(printf '%s\n' "$3"; shift 3; (( $# == 0 )) || printf '%s\n' "$@" | sort)
  got=$(read_catalog "${reader}" "${member}") || return 1
  [[ ${got} == "${want}" ]]
}

# assert_reads <reader> <member> <what> <status> [<provider model>...]
assert_reads() {
  local reader=$1 member=$2 what=$3
  shift 3
  eventually 120 "${reader} reads ${member}: ${what}" reads "${reader}" "${member}" "$@" \
    || note "${reader} read from ${member}: $(read_catalog "${reader}" "${member}" | tr '\n' ',' || true)"
}

# refused <reader> <member>: <member> serves <reader> no catalog. It ends the TLS handshake
# (curl 35, or 56 when TLS 1.3 rejects the client certificate after the handshake) or
# answers 403, whichever its listener has reached. A timeout, a refused connection, a
# malformed 200 or any other status is not a refusal.
refused() {
  local out rc=0
  out=$(read_catalog "$1" "$2" 2>/dev/null) || rc=$?
  case ${rc} in
    0) [[ ${out%%$'\n'*} == 403 ]] ;;
    35 | 56) return 0 ;;
    *) return 1 ;;
  esac
}

# offer <site> <provider> <model>...: <site> declares a provider serving exactly these models.
offer() {
  local site=$1 provider=$2 models
  shift 2
  models=$(printf '{name: %s}, ' "$@")
  ns "${site}" apply --server-side --force-conflicts --field-manager=e2e-acm -f - >/dev/null <<EOF || fail "${site}: cannot declare ${provider}"
apiVersion: grid.praxis.fast/v1alpha1
kind: InferenceProvider
metadata:
  name: ${provider}
spec:
  gridNetworkRef: ${NETWORK}
  providerKind: openai
  backendKind: llm-d
  endpoint: http://${provider}.${NS}.svc:8000
  models: [${models%, }]
EOF
}

hold_identity "${SITES[@]}"
assert_reads west east "no models before any are declared" 200
offer east pool alpha beta
assert_reads west east "both models of a new provider" 200 "pool alpha" "pool beta"
offer east pool alpha beta gamma
assert_reads west east "an added model" 200 "pool alpha" "pool beta" "pool gamma"
offer east pool alpha gamma
assert_reads west east "a removed model is gone" 200 "pool alpha" "pool gamma"
offer east spare delta
assert_reads west east "a second provider beside the first" 200 "pool alpha" "pool gamma" "spare delta"
ns east delete inferenceprovider/spare --wait --timeout=120s >/dev/null || fail "east: cannot remove spare"
assert_reads west east "a removed provider is gone" 200 "pool alpha" "pool gamma"
assert_reads east west "nothing from a site that offers nothing" 200
assert_polls west east

echo "== a third site joins the running grid =="
# The joiner comes up holding every record. The members hold none for it yet, and nothing it
# says adds one: until a member is delivered the joiner's record, it refuses the joiner. The
# same read succeeds below once the record is delivered, so a refusal here is not a dead path.
create_clusters "${JOINER}"
[[ ${IDENTITY} != certmanager ]] || install_cert_manager "${JOINER}"
give_identities "${JOINER}"
install_operators "${JOINER}"
read_endpoints "${JOINER}"
ALL=("${SITES[@]}" "${JOINER}")
deliver "${JOINER}" "${ALL[@]}"
assert_identities "${JOINER}"
assert_no_enrollment "${JOINER}"
hold_identity "${JOINER}"
offer "${JOINER}" edge epsilon
for site in "${SITES[@]}"; do
  refused "${JOINER}" "${site}" && pass "${site} serves ${JOINER} nothing while it holds no record for it" \
    || fail "${site} serves ${JOINER} its catalog with no record for it"
  if ns "${site}" get "gridsite/${JOINER}" >/dev/null 2>&1; then
    fail "${site}: holds a record for ${JOINER} that nothing delivered"
  else
    pass "${site}: no record for ${JOINER} until one is delivered"
  fi
done
for site in "${SITES[@]}"; do
  deliver "${site}" "${ALL[@]}"
done
for site in "${SITES[@]}"; do
  assert_polls "${site}" "${JOINER}"
  assert_polls "${JOINER}" "${site}"
  assert_reads "${site}" "${JOINER}" "the joiner's model" 200 "edge epsilon"
done
assert_reads "${JOINER}" east "the models east offers now, not the ones it dropped" 200 "pool alpha" "pool gamma"
assert_reads "${JOINER}" west "nothing from a site that offers nothing" 200

echo "== the third site is removed from the records =="
# Removed where the records are delivered. The departing site still holds its own records
# and still asks; the members stop answering it and stop asking it.
for site in "${SITES[@]}"; do
  ns "${site}" delete "gridsite/${JOINER}" --wait --timeout=120s >/dev/null \
    || fail "${site}: cannot remove ${JOINER}'s record"
done
# unreached <site> <member>: <site>'s last poll of <member> did not answer.
unreached() { [[ $(condition "$1" "gridsite/$2" SignalsReachable status) == False ]]; }
for site in "${SITES[@]}"; do
  eventually 120 "${site} serves ${JOINER} nothing once its record is removed" refused "${JOINER}" "${site}"
  eventually 120 "${JOINER}: its poll of ${site} no longer answers" unreached "${JOINER}" "${site}"
done
assert_reads west east "east's models, unchanged by the removal" 200 "pool alpha" "pool gamma"
for site in "${SITES[@]}"; do
  for member in "${SITES[@]}"; do
    [[ ${member} == "${site}" ]] || assert_polls "${site}" "${member}"
  done
  # Checked last, after the departed site has gone on asking: nothing it sends restores it.
  refused "${JOINER}" "${site}" && pass "${site} still serves ${JOINER} nothing" \
    || fail "${site} serves ${JOINER} its catalog again"
  if ns "${site}" get "gridsite/${JOINER}" >/dev/null 2>&1; then
    fail "${site}: ${JOINER}'s record came back without being delivered"
  else
    pass "${site}: ${JOINER} stays removed while it goes on asking"
  fi
  [[ $(ns "${site}" get gridsites -o json | jq -r '[.items[].metadata.name] | sort | join(" ")') == "${SITES[*]}" ]] \
    && pass "${site}: its members are exactly ${SITES[*]}" \
    || fail "${site}: its members are $(ns "${site}" get gridsites -o name | tr '\n' ' ')"
done

echo "== the last model is withdrawn =="
ns east delete inferenceprovider/pool --wait --timeout=120s >/dev/null || fail "east: cannot remove pool"
assert_reads west east "nothing once its last provider is removed" 200

if (( FAILED )); then
  echo "RESULT: FAIL"
  exit 1
fi
echo "RESULT: PASS"
