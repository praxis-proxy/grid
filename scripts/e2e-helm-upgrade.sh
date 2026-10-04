#!/usr/bin/env bash
# Chart upgrade e2e: install the grid charts from a base ref, then `helm upgrade`
# (with --take-ownership by default) to this checkout. Asserts every release upgrades, every CRD
# and CR survives with the same UID, and CRDs the branch renders become owned by
# the grid-operator release. When the branch moves the API group, CRs cannot carry
# across (new CRD names), so only the release upgrade and new-CRD ownership are checked.
#
# No operator runs (pause image, no --wait): this tests Helm release mechanics,
# not runtime behavior.
#
# Env: BASE_REF (default: merge-base with origin/main), INSTALL_HELM and
#      UPGRADE_HELM (helm binaries, default helm), CLUSTER (default
#      grid-helm-upgrade), KEEP=1 to skip teardown, TAKE_OWNERSHIP=0 for Helm
#      before 3.17, which lacks the flag; it runs the documented manual CRD
#      adoption instead when the branch renders CRDs. Local rootless podman runs
#      set KIND_CREATE_PREFIX like scripts/e2e-enrollment.sh. Needs bash 4.4+.
set -euo pipefail
# A failed list inside $(...) must fail the script, not vanish in a loop.
shopt -s inherit_errexit

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
BASE_REF="${BASE_REF:-$(git -C "${ROOT}" merge-base HEAD origin/main)}"
INSTALL_HELM="${INSTALL_HELM:-helm}"
UPGRADE_HELM="${UPGRADE_HELM:-helm}"
CLUSTER="${CLUSTER:-grid-helm-upgrade}"
UPGRADE_FLAGS=()
[ "${TAKE_OWNERSHIP:-1}" = "1" ] && UPGRADE_FLAGS=(--take-ownership)
CTX="kind-${CLUSTER}"
NS=grid-system
K="kubectl --context ${CTX}"
WORK="$(mktemp -d)"
BASE="${WORK}/base"
CREATED=0
cleanup() {
  if [ "${CREATED}" = "1" ] && [ "${KEEP:-0}" != "1" ]; then
    kind delete cluster --name "${CLUSTER}" >/dev/null 2>&1 || true
  fi
  git -C "${ROOT}" worktree remove --force "${BASE}" 2>/dev/null || true
  rm -rf "${WORK}"
}
trap cleanup EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }
pass() { echo "PASS: $*"; }

# Values both sides install with; the operator and gateway never start.
OPERATOR_ARGS=(--set image.repository=registry.k8s.io/pause --set image.tag=3.9)
SITE_ARGS=(--set gridNetwork.name=upgrade --set gridSite.name=upgrade-site)
GATEWAY_ARGS=(--set config.existingConfigMap=upgrade-gateway --set image.tag=v0.1.0-ci)
RELEASES=(grid-operator grid-site praxis-gateway grid-mock-providers)

release_args() { # <release>: prints the extra args, one per line
  case "$1" in
    grid-operator) printf '%s\n' "${OPERATOR_ARGS[@]}" ;;
    grid-site) printf '%s\n' "${SITE_ARGS[@]}" ;;
    praxis-gateway) printf '%s\n' "${GATEWAY_ARGS[@]}" ;;
  esac
}

uids() { # <kind> [-n ns | -A]: "name uid" lines, sorted
  ${K} get "$@" -o jsonpath='{range .items[*]}{.metadata.namespace}/{.metadata.name} {.metadata.uid}{"\n"}{end}' | sort
}

echo "== base ${BASE_REF}, install with $(${INSTALL_HELM} version --short), upgrade with $(${UPGRADE_HELM} version --short) =="
git -C "${ROOT}" worktree add --detach "${BASE}" "${BASE_REF}" >/dev/null

crd_group() { # <chart dir>: the grid API group its GridNetwork CRD serves
  awk '/^  group: /{print $2; exit}' "$1"/templates/crds/gridnetwork.yaml "$1"/crds/gridnetwork.yaml 2>/dev/null
}
BASE_GROUP="$(crd_group "${BASE}/charts/grid-operator")"
GROUP="$(crd_group "${ROOT}/charts/grid-operator")"
[ -n "${BASE_GROUP}" ] && [ -n "${GROUP}" ] || fail "cannot read the grid API group (base '${BASE_GROUP}', branch '${GROUP}')"
echo "== API group: base ${BASE_GROUP}, branch ${GROUP} =="

# Captured, not piped: under pipefail grep -q's early exit can fail the pipeline on a match.
clusters=$(kind get clusters 2>/dev/null || true)
if grep -qx "${CLUSTER}" <<<"${clusters}"; then
  fail "kind cluster '${CLUSTER}' already exists; refusing to clobber it. Delete it or set CLUSTER=<unique-name>."
fi
CREATED=1
${KIND_CREATE_PREFIX:-} kind create cluster --name "${CLUSTER}" --wait 60s

${K} create namespace "${NS}"
${K} -n "${NS}" create configmap upgrade-gateway --from-literal=config.yaml='{}'

echo "== install from base =="
for r in "${RELEASES[@]}"; do
  mapfile -t extra < <(release_args "${r}")
  ${INSTALL_HELM} --kube-context "${CTX}" install "${r}" "${BASE}/charts/${r}" -n "${NS}" "${extra[@]}" >/dev/null \
    || fail "base install of ${r}"
  pass "base install: ${r}"
done
# Out-of-band CRs (cluster-scoped), created against the base CRDs.
${K} apply -f "${BASE}/deploy/examples/single-cluster-api-provider/gridnetwork.yaml" \
  -f "${BASE}/deploy/examples/single-cluster-api-provider/gridsite.yaml" \
  -f "${BASE}/deploy/examples/single-cluster-api-provider/inference-provider.yaml" >/dev/null

CRDS_BEFORE="$(uids crd)"
CRDS_BEFORE="$(grep -F ".${BASE_GROUP} " <<<"${CRDS_BEFORE}" || true)"
CRS_BEFORE="$(for k in gridnetworks gridsites inferenceproviders; do uids "${k}.${BASE_GROUP}" -A; done)"
[ "$(wc -l <<<"${CRDS_BEFORE}")" -ge 4 ] || fail "expected the 4 grid CRDs after base install, got: ${CRDS_BEFORE}"
[ "$(wc -l <<<"${CRS_BEFORE}")" -ge 5 ] || fail "expected chart and example CRs after base install, got: ${CRS_BEFORE}"

# Documented one-time CRD adoption for Helm < 3.17 (keep in sync with the
# grid-operator README).
adopt_crds() { # <release> <namespace>
  for crd in agenttoolproviders gridnetworks gridsites inferenceproviders; do
    kubectl --context "${CTX}" label crd "${crd}.${GROUP}" \
      app.kubernetes.io/managed-by=Helm --overwrite
    kubectl --context "${CTX}" annotate crd "${crd}.${GROUP}" \
      meta.helm.sh/release-name="$1" meta.helm.sh/release-namespace="$2" --overwrite
  done
}
branch_crds="$(${UPGRADE_HELM} template grid-operator "${ROOT}/charts/grid-operator" -n "${NS}" "${OPERATOR_ARGS[@]}" \
  | grep -c '^kind: CustomResourceDefinition' || true)"
if [ "${TAKE_OWNERSHIP:-1}" != "1" ] && [ "${branch_crds}" -gt 0 ] && [ "${BASE_GROUP}" = "${GROUP}" ]; then
  echo "== branch renders ${branch_crds} CRDs; adopting them manually (Helm < 3.17) =="
  adopt_crds grid-operator "${NS}" >/dev/null
  pass "manual CRD adoption ran"
fi

echo "== upgrade to branch ${UPGRADE_FLAGS[*]} =="
for r in "${RELEASES[@]}"; do
  mapfile -t extra < <(release_args "${r}")
  ${UPGRADE_HELM} --kube-context "${CTX}" upgrade "${r}" "${ROOT}/charts/${r}" -n "${NS}" \
    "${UPGRADE_FLAGS[@]}" "${extra[@]}" >/dev/null || fail "upgrade of ${r}"
  status="$(${UPGRADE_HELM} --kube-context "${CTX}" status "${r}" -n "${NS}" -o json | python3 -c 'import sys,json;d=json.load(sys.stdin);print(d["info"]["status"],d["version"])')"
  [ "${status}" = "deployed 2" ] || fail "${r} is '${status}' after upgrade, want 'deployed 2'"
  pass "upgrade: ${r} (${status})"
done

if [ "${BASE_GROUP}" = "${GROUP}" ]; then
  echo "== CRDs and CRs survive =="
  CRDS_AFTER="$(uids crd)"
  CRDS_AFTER="$(grep -F ".${GROUP} " <<<"${CRDS_AFTER}" || true)"
  [ "${CRDS_AFTER}" = "${CRDS_BEFORE}" ] || fail "CRD set or UIDs changed across upgrade:
$(diff <(echo "${CRDS_BEFORE}") <(echo "${CRDS_AFTER}"))"
  pass "every grid CRD kept its UID ($(wc -l <<<"${CRDS_AFTER}"))"
  CRS_AFTER="$(for k in gridnetworks gridsites inferenceproviders; do uids "${k}.${GROUP}" -A; done)"
  [ "${CRS_AFTER}" = "${CRS_BEFORE}" ] || fail "CR set or UIDs changed across upgrade:
$(diff <(echo "${CRS_BEFORE}") <(echo "${CRS_AFTER}"))"
  pass "every CR kept its UID ($(wc -l <<<"${CRS_AFTER}"))"
else
  echo "SKIP: CR carry-over, since the API group moved from ${BASE_GROUP} to ${GROUP} (no in-place upgrade)"
fi

echo "== CRD ownership =="
manifest="$(${UPGRADE_HELM} --kube-context "${CTX}" get manifest grid-operator -n "${NS}")" && [ -n "${manifest}" ] \
  || fail "cannot read the grid-operator release manifest"
rendered="$(grep -c '^kind: CustomResourceDefinition' <<<"${manifest}" || true)"
if [ "${rendered}" -eq 0 ]; then
  pass "branch ships CRDs in crds/ (not release-managed); ownership check not applicable"
else
  for crd in $(${K} get crd -o name | grep -F ".${GROUP}"); do
    owner="$(${K} get "${crd}" -o jsonpath='{.metadata.annotations.meta\.helm\.sh/release-name}')"
    [ "${owner}" = "grid-operator" ] || fail "${crd} is not owned by the grid-operator release (owner '${owner}')"
  done
  # Annotations alone can be hand-set; the release manifest must list every grid CRD.
  in_cluster="$(${K} get crd -o name | grep -F ".${GROUP}" | sed 's|.*/||' | sort)"
  in_release="$(awk '/^---/{crd=0} /^kind: CustomResourceDefinition/{crd=1} crd && /^  name: /{print $2; crd=0}' <<<"${manifest}" | sort)"
  [ "${in_cluster}" = "${in_release}" ] || fail "release manifest CRDs differ from the cluster's grid CRDs:
$(diff <(echo "${in_cluster}") <(echo "${in_release}"))"
  pass "all ${rendered} rendered CRDs are owned by and listed in the grid-operator release"
fi

echo "== e2e GREEN: base ${BASE_REF} upgrades to the branch =="
