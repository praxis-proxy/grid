#!/usr/bin/env bash

# Ownership plan for the fixed-name clusters used by combined-site Forge.
COMBINED_SITE_EXPECTED_CLUSTERS=(
  grid-combined-west
  grid-combined-central
  grid-combined-east
)
COMBINED_SITE_EXPECTED_NETWORK=grid-combined-site-net
# These arrays are consumed by the workflow after this library is sourced.
# shellcheck disable=SC2034
COMBINED_SITE_OWNED_CLUSTERS=()
# shellcheck disable=SC2034
COMBINED_SITE_OWNED_NETWORKS=()
# shellcheck disable=SC2034
COMBINED_SITE_OWNED_KIND_TEMP_DIRS=()
# shellcheck disable=SC2034
COMBINED_SITE_CLEANUP_RESULT=success

combined_site_inventory_is_valid() {
  local inventory_file="$1"
  [[ -f "${inventory_file}" && ! -L "${inventory_file}" ]] || return 1

  local entry
  local -A seen=()
  while IFS= read -r entry || [[ -n "${entry}" ]]; do
    [[ "${entry}" =~ ^[a-zA-Z0-9][a-zA-Z0-9._-]*$ ]] || return 1
    [[ -z "${seen[${entry}]+present}" ]] || return 1
    seen["${entry}"]=present
  done < "${inventory_file}"
}

combined_site_state_is_valid() {
  local state_file="$1"
  [[ -f "${state_file}" && ! -L "${state_file}" ]] || return 1

  jq -e '
    type == "object"
    and (.clusters | type == "array")
    and (.network | type == "object" and .name == "grid-combined-site-net")
    and (.networkCreatedByForge | type == "boolean")
    and ([.clusters[].name] | length == (unique | length))
    and all(.clusters[];
      (.name == "west" and .kindName == "grid-combined-west" and .context == "kind-grid-combined-west")
      or (.name == "central" and .kindName == "grid-combined-central" and .context == "kind-grid-combined-central")
      or (.name == "east" and .kindName == "grid-combined-east" and .context == "kind-grid-combined-east")
    )
  ' "${state_file}" > /dev/null 2>&1
}

combined_site_inventory_contains() {
  local inventory_file="$1"
  local resource_name="$2"
  grep -Fxq -- "${resource_name}" "${inventory_file}"
}

# Record only fixed-name Kind temp paths, including symlinks and non-directory
# entries, so pre-existing paths make the workflow refuse to start.
combined_site_record_kind_temp_inventory() {
  local temporary_root="$1"
  local output_file="$2"
  [[ "${temporary_root}" == /* && -d "${temporary_root}" && ! -L "${temporary_root}" ]] || return 1
  [[ ! -e "${output_file}" && ! -L "${output_file}" ]] || return 1
  : > "${output_file}"

  local cluster temporary_path
  for cluster in "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}"; do
    temporary_path="${temporary_root%/}/kind-${cluster}"
    if [[ -e "${temporary_path}" || -L "${temporary_path}" ]]; then
      printf '%s\n' "${cluster}" >> "${output_file}"
    fi
  done
}

combined_site_baseline_is_clean() {
  local evidence_dir="$1"
  local clusters_file="${evidence_dir}/kind-clusters-before.txt"
  local networks_file="${evidence_dir}/docker-networks-before.txt"
  local temporary_paths_file="${evidence_dir}/kind-temporary-paths-before.txt"

  if ! combined_site_inventory_is_valid "${clusters_file}" \
    || ! combined_site_inventory_is_valid "${networks_file}" \
    || ! combined_site_kind_temp_inventory_is_valid "${temporary_paths_file}"; then
    echo "::error::Combined-site baseline inventory is absent or malformed; refusing to start"
    return 1
  fi

  local cluster
  for cluster in "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}"; do
    if combined_site_inventory_contains "${clusters_file}" "${cluster}"; then
      echo "::error::Refusing combined-site qualification because fixed-name cluster '${cluster}' already exists on this runner. Preserve it and use a clean runner."
      return 1
    fi
    if combined_site_inventory_contains "${temporary_paths_file}" "${cluster}"; then
      echo "::error::Refusing combined-site qualification because temporary Kind path for '${cluster}' already exists on this runner. Preserve it and use a clean runner."
      return 1
    fi
  done

  if combined_site_inventory_contains "${networks_file}" "${COMBINED_SITE_EXPECTED_NETWORK}"; then
    echo "::error::Refusing combined-site qualification because fixed-name network '${COMBINED_SITE_EXPECTED_NETWORK}' already exists on this runner. Preserve it and use a clean runner."
    return 1
  fi
  return 0
}

combined_site_kind_temp_inventory_is_valid() {
  local inventory_file="$1"
  combined_site_inventory_is_valid "${inventory_file}" || return 1

  local cluster
  while IFS= read -r cluster || [[ -n "${cluster}" ]]; do
    case " ${COMBINED_SITE_EXPECTED_CLUSTERS[*]} " in
      *" ${cluster} "*) ;;
      *) return 1 ;;
    esac
  done < "${inventory_file}"
}

# Populate owned-resource arrays from this run's Forge state. Baseline inventories
# prove the preflight ran; when Forge state is unavailable, only unchanged
# pre-existing resources are considered safe to leave alone.
combined_site_cleanup_plan() {
  local evidence_dir="$1"
  local state_file="$2"
  local current_clusters_file="$3"
  local current_networks_file="$4"
  local current_kind_temps_file="$5"
  local before_clusters_file="${evidence_dir}/kind-clusters-before.txt"
  local before_networks_file="${evidence_dir}/docker-networks-before.txt"
  local before_kind_temps_file="${evidence_dir}/kind-temporary-paths-before.txt"

  COMBINED_SITE_OWNED_CLUSTERS=()
  COMBINED_SITE_OWNED_NETWORKS=()
  COMBINED_SITE_OWNED_KIND_TEMP_DIRS=()
  COMBINED_SITE_CLEANUP_RESULT=success

  if ! combined_site_inventory_is_valid "${before_clusters_file}" \
    || ! combined_site_inventory_is_valid "${before_networks_file}" \
    || ! combined_site_inventory_is_valid "${current_clusters_file}" \
    || ! combined_site_inventory_is_valid "${current_networks_file}" \
    || ! combined_site_kind_temp_inventory_is_valid "${before_kind_temps_file}" \
    || ! combined_site_kind_temp_inventory_is_valid "${current_kind_temps_file}"; then
    echo "::warning::Combined-site ownership inventory is absent or malformed; preserving fixed-name resources"
    COMBINED_SITE_CLEANUP_RESULT=partial
    return 0
  fi

  if [[ -z "${state_file}" ]] || ! combined_site_state_is_valid "${state_file}"; then
    for cluster in "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}"; do
      if combined_site_inventory_contains "${current_clusters_file}" "${cluster}" \
        && ! combined_site_inventory_contains "${before_clusters_file}" "${cluster}"; then
        echo "::warning::Current combined-site cluster '${cluster}' has no valid run ownership record; preserving it"
        COMBINED_SITE_CLEANUP_RESULT=partial
      fi
    done
    if combined_site_inventory_contains "${current_networks_file}" "${COMBINED_SITE_EXPECTED_NETWORK}" \
      && ! combined_site_inventory_contains "${before_networks_file}" "${COMBINED_SITE_EXPECTED_NETWORK}"; then
      echo "::warning::Current combined-site network has no valid run ownership record; preserving it"
      COMBINED_SITE_CLEANUP_RESULT=partial
    fi
    while IFS= read -r cluster || [[ -n "${cluster}" ]]; do
      if ! combined_site_inventory_contains "${before_kind_temps_file}" "${cluster}"; then
        echo "::warning::Current Kind temp path for '${cluster}' has no valid run ownership record; preserving it"
        COMBINED_SITE_CLEANUP_RESULT=partial
      fi
    done < "${current_kind_temps_file}"
    if [[ "${COMBINED_SITE_CLEANUP_RESULT}" == success ]]; then
      echo "::notice::No newly created combined-site resources need cleanup"
    fi
    return 0
  fi

  local -a recorded_clusters=()
  mapfile -t recorded_clusters < <(jq -r '.clusters[].kindName' "${state_file}")
  for cluster in "${COMBINED_SITE_EXPECTED_CLUSTERS[@]}"; do
    if combined_site_inventory_contains "${current_clusters_file}" "${cluster}"; then
      if printf '%s\n' "${recorded_clusters[@]}" | grep -Fxq -- "${cluster}"; then
        COMBINED_SITE_OWNED_CLUSTERS+=("${cluster}")
      else
        echo "::warning::Current combined-site cluster '${cluster}' is absent from run Forge state; preserving all uncertain resources"
        COMBINED_SITE_OWNED_CLUSTERS=()
        COMBINED_SITE_OWNED_NETWORKS=()
        COMBINED_SITE_CLEANUP_RESULT=partial
        return 0
      fi
    fi
  done

  while IFS= read -r cluster || [[ -n "${cluster}" ]]; do
    if printf '%s\n' "${recorded_clusters[@]}" | grep -Fxq -- "${cluster}"; then
      COMBINED_SITE_OWNED_KIND_TEMP_DIRS+=("${cluster}")
    else
      echo "::warning::Current Kind temp path for '${cluster}' is absent from run Forge state; preserving it"
      COMBINED_SITE_CLEANUP_RESULT=partial
    fi
  done < "${current_kind_temps_file}"

  if combined_site_inventory_contains "${current_networks_file}" "${COMBINED_SITE_EXPECTED_NETWORK}"; then
    if jq -e '.networkCreatedByForge == true' "${state_file}" > /dev/null 2>&1; then
      # shellcheck disable=SC2034
      COMBINED_SITE_OWNED_NETWORKS=("${COMBINED_SITE_EXPECTED_NETWORK}")
    else
      echo "::warning::Current combined-site network is not recorded as Forge-created; preserving it"
      COMBINED_SITE_CLEANUP_RESULT=partial
    fi
  fi
}

# Remove Kind's temporary directory only for a cluster whose deletion was
# verified by the caller. The root is injectable so tests never touch /tmp.
combined_site_remove_kind_temporary_files() {
  local temporary_root="$1"
  local current_clusters_file="$2"
  shift 2
  [[ "${temporary_root}" == /* && -d "${temporary_root}" && ! -L "${temporary_root}" ]] || return 1
  combined_site_inventory_is_valid "${current_clusters_file}" || return 1

  local cluster temporary_dir
  for cluster in "$@"; do
    case " ${COMBINED_SITE_EXPECTED_CLUSTERS[*]} " in
      *" ${cluster} "*) ;;
      *) return 1 ;;
    esac
    if combined_site_inventory_contains "${current_clusters_file}" "${cluster}"; then
      echo "::warning::Kind cluster '${cluster}' still exists; preserving its temporary files"
      return 1
    fi
    temporary_dir="${temporary_root%/}/kind-${cluster}"
    if [[ -L "${temporary_dir}" ]]; then
      return 1
    fi
    if [[ -d "${temporary_dir}" ]] && ! rm -rf -- "${temporary_dir}"; then
      return 1
    fi
  done
}
