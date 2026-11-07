#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
# The helper is loaded explicitly from SCRIPT_DIR by this test.
# shellcheck disable=SC1091
source "${SCRIPT_DIR}/combined-site-cleanup-ownership.sh"

ROOT=$(mktemp -d)
trap 'rm -rf -- "${ROOT}"' EXIT

fail() {
  printf 'FAIL: %s\n' "$*" >&2
  exit 1
}

assert_plan() {
  local expected_result="$1"
  local expected_clusters="$2"
  local expected_networks="$3"
  [[ "${COMBINED_SITE_CLEANUP_RESULT}" == "${expected_result}" ]] || fail \
    "expected cleanup result ${expected_result}, got ${COMBINED_SITE_CLEANUP_RESULT}"
  local actual_clusters actual_networks
  actual_clusters=$(printf '%s\n' "${COMBINED_SITE_OWNED_CLUSTERS[@]}")
  actual_networks=$(printf '%s\n' "${COMBINED_SITE_OWNED_NETWORKS[@]}")
  [[ "${actual_clusters}" == "${expected_clusters}" ]] || fail \
    "expected owned clusters '${expected_clusters}', got '${actual_clusters}'"
  [[ "${actual_networks}" == "${expected_networks}" ]] || fail \
    "expected owned networks '${expected_networks}', got '${actual_networks}'"
}

new_case() {
  CASE_DIR="${ROOT}/case-$1"
  mkdir -p "${CASE_DIR}"
  : > "${CASE_DIR}/kind-clusters-before.txt"
  : > "${CASE_DIR}/docker-networks-before.txt"
  : > "${CASE_DIR}/kind-clusters-now.txt"
  : > "${CASE_DIR}/docker-networks-now.txt"
  STATE_FILE="${CASE_DIR}/state.json"
}

write_full_state() {
  local network_created="$1"
  cat > "${STATE_FILE}" <<EOF
{"clusters":[
 {"name":"west","kindName":"grid-combined-west","context":"kind-grid-combined-west","phase":"running"},
 {"name":"central","kindName":"grid-combined-central","context":"kind-grid-combined-central","phase":"running"},
 {"name":"east","kindName":"grid-combined-east","context":"kind-grid-combined-east","phase":"running"}
],"network":{"name":"grid-combined-site-net","phase":"active"},"networkCreatedByForge":${network_created}}
EOF
}

# Fresh baseline: only resources recorded by this run are eligible for cleanup.
new_case no-preexisting
printf '%s\n' "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}" > "${CASE_DIR}/kind-clusters-now.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-now.txt"
write_full_state true
combined_site_cleanup_plan "${CASE_DIR}" "${STATE_FILE}" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan success $'grid-combined-west\ngrid-combined-central\ngrid-combined-east' \
  "${COMBINED_SITE_EXPECTED_NETWORK}"

# A prior fixed-name cluster and network are preserved, while verified new
# resources from this run remain eligible for cleanup.
new_case preexisting
printf '%s\n' grid-combined-west > "${CASE_DIR}/kind-clusters-before.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-before.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}" > "${CASE_DIR}/kind-clusters-now.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-now.txt"
write_full_state true
combined_site_cleanup_plan "${CASE_DIR}" "${STATE_FILE}" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan success $'grid-combined-central\ngrid-combined-east' ""

# Cluster ownership is independent of network ownership. Preserve an uncertain
# network while still allowing deletion of clusters proven new by this run.
new_case uncertain-network-ownership
printf '%s\n' "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}" > "${CASE_DIR}/kind-clusters-now.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-now.txt"
write_full_state false
combined_site_cleanup_plan "${CASE_DIR}" "${STATE_FILE}" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan partial $'grid-combined-west\ngrid-combined-central\ngrid-combined-east' ""

# Missing and malformed inventories fail closed; the currently present
# resources are not returned as deletable.
new_case missing-inventory
rm "${CASE_DIR}/kind-clusters-before.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}" > "${CASE_DIR}/kind-clusters-now.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-now.txt"
write_full_state true
combined_site_cleanup_plan "${CASE_DIR}" "${STATE_FILE}" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan partial "" ""

new_case malformed-inventory
printf '%s\n' grid-combined-west grid-combined-west > "${CASE_DIR}/kind-clusters-before.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}" > "${CASE_DIR}/kind-clusters-now.txt"
write_full_state true
combined_site_cleanup_plan "${CASE_DIR}" "${STATE_FILE}" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan partial "" ""

new_case malformed-state
printf '%s\n' "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}" > "${CASE_DIR}/kind-clusters-now.txt"
printf '%s\n' '{broken json' > "${STATE_FILE}"
combined_site_cleanup_plan "${CASE_DIR}" "${STATE_FILE}" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan partial "" ""

# Failure before the qualification begins has no state. Known pre-existing
# resources are preserved; newly appeared fixed-name resources are uncertain.
new_case before-qualification
printf '%s\n' grid-combined-west > "${CASE_DIR}/kind-clusters-before.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-before.txt"
printf '%s\n' grid-combined-west > "${CASE_DIR}/kind-clusters-now.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-now.txt"
combined_site_cleanup_plan "${CASE_DIR}" "" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan success "" ""

new_case before-qualification-uncertain
printf '%s\n' grid-combined-west > "${CASE_DIR}/kind-clusters-now.txt"
combined_site_cleanup_plan "${CASE_DIR}" "" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan partial "" ""

# If this run created resources and then failed, its unique Forge state and the
# empty pre-run inventory still authorize cleanup after diagnostics are saved.
new_case failed-after-create
printf '%s\n' "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}" > "${CASE_DIR}/kind-clusters-now.txt"
printf '%s\n' "${COMBINED_SITE_EXPECTED_NETWORK}" > "${CASE_DIR}/docker-networks-now.txt"
write_full_state true
combined_site_cleanup_plan "${CASE_DIR}" "${STATE_FILE}" \
  "${CASE_DIR}/kind-clusters-now.txt" "${CASE_DIR}/docker-networks-now.txt"
assert_plan success $'grid-combined-west\ngrid-combined-central\ngrid-combined-east' \
  "${COMBINED_SITE_EXPECTED_NETWORK}"

# Temporary Kind files follow the verified deleted-cluster list, not the
# topology's fixed names. The pre-existing west fixture remains untouched.
TEMP_ROOT="${ROOT}/kind-temp"
mkdir -p "${TEMP_ROOT}/kind-grid-combined-west" "${TEMP_ROOT}/kind-grid-combined-central"
combined_site_remove_kind_temporary_files "${TEMP_ROOT}" grid-combined-central
[[ -d "${TEMP_ROOT}/kind-grid-combined-west" ]] || fail \
  "pre-existing cluster temporary files must be preserved"
[[ ! -e "${TEMP_ROOT}/kind-grid-combined-central" ]] || fail \
  "temporary files for a verified deleted cluster must be removed"

printf 'PASS: combined-site cleanup ownership cases\n'
