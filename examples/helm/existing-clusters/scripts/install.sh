#!/usr/bin/env bash
# Install Grid components onto existing clusters using Helm.
# Reads a local inventory file for cluster contexts and topology.
#
# Usage: ./install.sh <inventory.yaml> [--site-values SITE:ROLE:PATH ...]
#
# Override files are applied after the repository example values so
# user-provided values win.  Valid roles: operator, site, mock,
# consumer, provider, provider-config, consumer-config.
#
# Requires: kubectl, helm, yq, python3

set -euo pipefail

# Argument parsing

if [[ $# -lt 1 ]]; then
  echo "Usage: $0 <inventory.yaml> [--site-values SITE:ROLE:PATH ...]" >&2
  exit 1
fi

INVENTORY="$1"
shift

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory file not found: $INVENTORY" >&2
  exit 1
fi

declare -A SITE_OVERRIDES
OVERRIDE_COUNT=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --site-values)
      if [[ $# -lt 2 ]]; then
        echo "ERROR: --site-values requires an argument (SITE:ROLE:PATH)" >&2
        exit 1
      fi
      SITE_OVERRIDES_RAW="${2}"
      IFS=: read -r OV_SITE OV_ROLE OV_PATH <<< "$SITE_OVERRIDES_RAW"
      if [[ -z "$OV_SITE" || -z "$OV_ROLE" || -z "$OV_PATH" ]]; then
        echo "ERROR: --site-values must be SITE:ROLE:PATH, got: $SITE_OVERRIDES_RAW" >&2
        exit 1
      fi
      OV_KEY="${OV_SITE}:${OV_ROLE}"
      if [[ -v "SITE_OVERRIDES[$OV_KEY]" ]]; then
        echo "ERROR: duplicate --site-values for ${OV_KEY}" >&2
        exit 1
      fi
      SITE_OVERRIDES["$OV_KEY"]="$OV_PATH"
      OVERRIDE_COUNT=$((OVERRIDE_COUNT + 1))
      shift 2
      ;;
    *)
      echo "ERROR: unknown argument: $1" >&2
      echo "Usage: $0 <inventory.yaml> [--site-values SITE:ROLE:PATH ...]" >&2
      exit 1
      ;;
  esac
done

# Inventory and topology

TOPOLOGY=$(yq eval '.topology' "$INVENTORY")
CHART_DIR="$(cd "$(dirname "$0")/../../../.." && pwd)/charts"
VALUES_DIR="$(cd "$(dirname "$0")/.." && pwd)/${TOPOLOGY}/values"

CUSTOM_VALUES_DIR=$(yq eval '.valuesDir // ""' "$INVENTORY")
if [[ -n "$CUSTOM_VALUES_DIR" ]]; then
  if [[ ! -d "$CUSTOM_VALUES_DIR" ]]; then
    echo "ERROR: valuesDir not found: $CUSTOM_VALUES_DIR" >&2
    exit 1
  fi
  VALUES_DIR="$CUSTOM_VALUES_DIR"
elif [[ ! -d "$VALUES_DIR" ]]; then
  echo "ERROR: values directory not found for topology '$TOPOLOGY': $VALUES_DIR" >&2
  exit 1
fi

SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")

# Validate overrides before any cluster changes

VALID_ROLES="operator site mock consumer provider provider-config consumer-config"

for OV_KEY in "${!SITE_OVERRIDES[@]+"${!SITE_OVERRIDES[@]}"}"; do
  [[ -z "$OV_KEY" ]] && continue
  IFS=: read -r OV_SITE OV_ROLE <<< "$OV_KEY"
  OV_PATH="${SITE_OVERRIDES[$OV_KEY]}"

  # Site must exist in inventory.
  if ! echo "$SITE_NAMES" | grep -qw "$OV_SITE"; then
    echo "ERROR: --site-values site '$OV_SITE' not found in inventory" >&2
    exit 1
  fi

  # Role must be valid.
  if ! echo "$VALID_ROLES" | grep -qw "$OV_ROLE"; then
    echo "ERROR: --site-values role '$OV_ROLE' must be one of: $VALID_ROLES" >&2
    exit 1
  fi

  # Path must be a readable regular file.
  if [[ ! -f "$OV_PATH" ]]; then
    echo "ERROR: --site-values file not found: $OV_PATH" >&2
    exit 1
  fi
  if [[ ! -r "$OV_PATH" ]]; then
    echo "ERROR: --site-values file not readable: $OV_PATH" >&2
    exit 1
  fi
  SITE_OVERRIDES["$OV_KEY"]="$OV_PATH"

  # For dedicated-edge, validate role matches the site's declared role.
  if [[ "$TOPOLOGY" == "dedicated-edge" && "$OV_ROLE" != "operator" ]]; then
    SITE_ROLES=$(yq eval ".sites.${OV_SITE}.roles[]" "$INVENTORY" 2>/dev/null || echo "")
    if [[ "$OV_ROLE" == "consumer" ]] && ! echo "$SITE_ROLES" | grep -q "consumer"; then
      echo "ERROR: site '$OV_SITE' has no consumer role in dedicated-edge topology" >&2
      exit 1
    fi
    if [[ "$OV_ROLE" == "provider" ]] && ! echo "$SITE_ROLES" | grep -q "provider"; then
      echo "ERROR: site '$OV_SITE' has no provider role in dedicated-edge topology" >&2
      exit 1
    fi
  fi
done

# Image overrides from inventory

IMAGE_SETS=()
OP_REPO=$(yq eval '.images.operator.repository // ""' "$INVENTORY")
OP_TAG=$(yq eval '.images.operator.tag // ""' "$INVENTORY")
GW_REPO=$(yq eval '.images.gateway.repository // ""' "$INVENTORY")
GW_TAG=$(yq eval '.images.gateway.tag // ""' "$INVENTORY")
[[ -n "$OP_REPO" ]] && IMAGE_SETS+=(--set "image.repository=$OP_REPO")
[[ -n "$OP_TAG" ]]  && IMAGE_SETS+=(--set "image.tag=$OP_TAG")

GW_SETS=()
[[ -n "$GW_REPO" ]] && GW_SETS+=(--set "image.repository=$GW_REPO")
[[ -n "$GW_TAG" ]]  && GW_SETS+=(--set "image.tag=$GW_TAG")

# Helper: resolve override for a site:role

get_override_args() {
  local site="$1" role="$2"
  local key="${site}:${role}"
  if [[ -v "SITE_OVERRIDES[$key]" ]]; then
    local path="${SITE_OVERRIDES[$key]}"
    local digest
    digest=$(sha256sum "$path" | cut -d' ' -f1)
    echo "  Override: ${key} sha256=${digest} (${path})" >&2
    echo "--values" "$path"
  fi
}

# Helper: wait for overlay ConfigMap

wait_for_overlay() {
  local context="$1" timeout="${2:-120}"
  local elapsed=0
  echo "  Waiting for overlay ConfigMap (up to ${timeout}s)..."
  while (( elapsed < timeout )); do
    if kubectl --context "$context" -n grid-system \
      get configmap -l grid.praxis.fast/network \
      -o name 2>/dev/null | grep -q configmap; then
      echo "  Overlay ConfigMap found."
      return 0
    fi
    sleep 5
    elapsed=$((elapsed + 5))
  done
  echo "  WARN: overlay ConfigMap not found after ${timeout}s; consumer gateway may fail to start" >&2
  return 0
}

# Helper: render provider config using overlay stable IDs

render_provider_config() {
  local context="$1" template="$2"

  local overlay_json
  overlay_json=$(kubectl --context "$context" -n grid-system \
    get configmap -l grid.praxis.fast/network \
    -o jsonpath='{.items[0].data.routing-config\.json}' 2>/dev/null)

  if [[ -z "$overlay_json" ]]; then
    echo "ERROR: cannot read overlay ConfigMap data" >&2
    return 1
  fi

  local pairs
  pairs=$(echo "$overlay_json" | python3 -c "
import json, sys
data = json.load(sys.stdin)
for c in data.get('candidates', []):
    print(c['cluster'] + '\t' + c['stable_id'])
")

  if [[ -z "$pairs" ]]; then
    echo "ERROR: no candidates found in overlay" >&2
    return 1
  fi

  local rendered
  rendered=$(cat "$template")

  while IFS=$'\t' read -r cluster stable_id; do
    [[ -z "$cluster" ]] && continue
    rendered=$(echo "$rendered" | yq eval \
      "(.filter_chains[].filters[] | select(.filter == \"provider_route\") | .routes[] | select(.candidate_id == \"$cluster\")).candidate_id = \"$stable_id\"" -)
    echo "    Overlay: ${cluster} -> ${stable_id}" >&2
  done <<< "$pairs"

  echo "$rendered"
}

# Run preflight

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PREFLIGHT="${SCRIPT_DIR}/preflight.sh"
if [[ ! -f "$PREFLIGHT" ]]; then
  echo "ERROR: preflight.sh not found at $PREFLIGHT" >&2
  exit 1
fi
echo "Running preflight checks..."
bash "$PREFLIGHT" "$INVENTORY"
echo ""

# Install

echo "Installing Grid components"
echo "  Topology: $TOPOLOGY"
echo "  Charts:   $CHART_DIR"
echo "  Values:   $VALUES_DIR"
if [[ "$OVERRIDE_COUNT" -gt 0 ]]; then
  echo "  Overrides: $OVERRIDE_COUNT file(s)"
fi
echo ""

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  ROLES=$(yq eval ".sites.${SITE}.roles[]" "$INVENTORY" 2>/dev/null || echo "")

  echo "--- Site: $SITE (context: $CONTEXT) ---"

  echo "  Creating namespace grid-system..."
  kubectl --context "$CONTEXT" create namespace grid-system --dry-run=client -o yaml \
    | kubectl --context "$CONTEXT" apply -f -

  # Build SWIM seeds from all other sites' swimAddress.
  SWIM_SEEDS=""
  for PEER in $SITE_NAMES; do
    [[ "$PEER" == "$SITE" ]] && continue
    PEER_ADDR=$(yq eval ".sites.${PEER}.swimAddress // \"\"" "$INVENTORY")
    if [[ -n "$PEER_ADDR" && "$PEER_ADDR" != "replace-me" ]]; then
      [[ -n "$SWIM_SEEDS" ]] && SWIM_SEEDS="${SWIM_SEEDS},"
      SWIM_SEEDS="${SWIM_SEEDS}${PEER_ADDR}:7946"
    fi
  done

  # Operator

  OPERATOR_VALUES="${VALUES_DIR}/${SITE}-operator.yaml"
  if [[ -f "$OPERATOR_VALUES" ]]; then
    SEED_SETS=()
    [[ -n "$SWIM_SEEDS" ]] && SEED_SETS+=(--set "swim.seeds=${SWIM_SEEDS}")

    OPERATOR_OV=()
    OV_ARGS=$(get_override_args "$SITE" "operator")
    if [[ -n "$OV_ARGS" ]]; then
      read -ra OPERATOR_OV <<< "$OV_ARGS"
    fi

    echo "  Installing grid-operator..."
    helm upgrade --install grid-operator "$CHART_DIR/grid-operator" \
      --kube-context "$CONTEXT" \
      --namespace grid-system \
      --values "$OPERATOR_VALUES" \
      "${OPERATOR_OV[@]+"${OPERATOR_OV[@]}"}" \
      "${IMAGE_SETS[@]+"${IMAGE_SETS[@]}"}" \
      "${SEED_SETS[@]+"${SEED_SETS[@]}"}" \
      --wait --timeout 120s
  else
    echo "  WARN: no operator values at $OPERATOR_VALUES, skipping" >&2
  fi

  # Mock inference providers (before grid-site so backends are
  # healthy when InferenceProvider CRs trigger health checks)

  MOCK_VALUES="${VALUES_DIR}/${SITE}-grid-mock-providers.yaml"
  if [[ -f "$MOCK_VALUES" ]]; then
    MOCK_OV=()
    OV_ARGS=$(get_override_args "$SITE" "mock")
    if [[ -n "$OV_ARGS" ]]; then
      read -ra MOCK_OV <<< "$OV_ARGS"
    fi

    echo "  Installing grid-mock-providers..."
    helm upgrade --install grid-mock-providers "$CHART_DIR/grid-mock-providers" \
      --kube-context "$CONTEXT" \
      --namespace grid-system \
      --values "$MOCK_VALUES" \
      "${MOCK_OV[@]+"${MOCK_OV[@]}"}" \
      --wait --timeout 120s
  fi

  # Site topology CRs

  SITE_VALUES="${VALUES_DIR}/${SITE}-grid-site.yaml"
  if [[ -f "$SITE_VALUES" ]]; then
    SITE_OV=()
    OV_ARGS=$(get_override_args "$SITE" "site")
    if [[ -n "$OV_ARGS" ]]; then
      read -ra SITE_OV <<< "$OV_ARGS"
    fi

    echo "  Installing grid-site..."
    helm upgrade --install grid-site "$CHART_DIR/grid-site" \
      --kube-context "$CONTEXT" \
      --namespace grid-system \
      --values "$SITE_VALUES" \
      "${SITE_OV[@]+"${SITE_OV[@]}"}" \
      --wait --timeout 120s
  fi

  # Gateways

  if [[ "$TOPOLOGY" == "dedicated-edge" ]]; then
    GATEWAY_VALUES="${VALUES_DIR}/${SITE}-gateway.yaml"
    if [[ -f "$GATEWAY_VALUES" ]]; then
      RELEASE_NAME="gateway"
      OVERRIDE_ROLE="consumer"
      if echo "$ROLES" | grep -q "consumer"; then
        RELEASE_NAME="consumer-gateway"
        OVERRIDE_ROLE="consumer"
      elif echo "$ROLES" | grep -q "provider"; then
        RELEASE_NAME="provider-gateway"
        OVERRIDE_ROLE="provider"
      fi

      GW_OV=()
      OV_ARGS=$(get_override_args "$SITE" "$OVERRIDE_ROLE")
      if [[ -n "$OV_ARGS" ]]; then
        read -ra GW_OV <<< "$OV_ARGS"
      fi

      echo "  Installing $RELEASE_NAME..."
      helm upgrade --install "$RELEASE_NAME" "$CHART_DIR/praxis-gateway" \
        --kube-context "$CONTEXT" \
        --namespace grid-system \
        --values "$GATEWAY_VALUES" \
        "${GW_OV[@]+"${GW_OV[@]}"}" \
        "${GW_SETS[@]+"${GW_SETS[@]}"}" \
        --wait --timeout 120s
    fi

  elif [[ "$TOPOLOGY" == "combined-site" ]]; then
    # Combined-site install sequence:
    # 1. Wait for overlay (generated after grid-site CRs)
    # 2. Render provider Praxis config with overlay stable IDs
    # 3. Create consumer Praxis config ConfigMap
    # 4. Install provider-gateway
    # 5. Install consumer-gateway

    wait_for_overlay "$CONTEXT" 120

    # Render provider Praxis config from overlay
    PROVIDER_CONFIG=""
    if [[ -v "SITE_OVERRIDES[${SITE}:provider-config]" ]]; then
      PROVIDER_CONFIG="${SITE_OVERRIDES[${SITE}:provider-config]}"
    elif [[ -f "${VALUES_DIR}/${SITE}-provider-praxis.yaml" ]]; then
      PROVIDER_CONFIG="${VALUES_DIR}/${SITE}-provider-praxis.yaml"
    fi

    if [[ -n "$PROVIDER_CONFIG" ]]; then
      echo "  Rendering provider Praxis config from overlay..."
      if ! RENDERED_PROVIDER=$(render_provider_config "$CONTEXT" "$PROVIDER_CONFIG"); then
        echo "ERROR: failed to render provider config" >&2
        exit 1
      fi
      kubectl --context "$CONTEXT" -n grid-system create configmap provider-praxis-config \
        --from-literal=praxis.yaml="$RENDERED_PROVIDER" \
        --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f -
    fi

    # Create consumer Praxis config ConfigMap
    CONSUMER_CONFIG=""
    if [[ -v "SITE_OVERRIDES[${SITE}:consumer-config]" ]]; then
      CONSUMER_CONFIG="${SITE_OVERRIDES[${SITE}:consumer-config]}"
    elif [[ -f "${VALUES_DIR}/${SITE}-consumer-praxis.yaml" ]]; then
      CONSUMER_CONFIG="${VALUES_DIR}/${SITE}-consumer-praxis.yaml"
    fi

    if [[ -n "$CONSUMER_CONFIG" ]]; then
      echo "  Creating consumer Praxis config..."
      kubectl --context "$CONTEXT" -n grid-system create configmap consumer-praxis-config \
        --from-file=praxis.yaml="$CONSUMER_CONFIG" \
        --dry-run=client -o yaml | kubectl --context "$CONTEXT" apply -f -
    fi

    # Provider gateway
    PROVIDER_VALUES="${VALUES_DIR}/${SITE}-provider-gateway.yaml"
    if [[ -f "$PROVIDER_VALUES" ]]; then
      PROVIDER_OV=()
      OV_ARGS=$(get_override_args "$SITE" "provider")
      if [[ -n "$OV_ARGS" ]]; then
        read -ra PROVIDER_OV <<< "$OV_ARGS"
      fi

      echo "  Installing provider-gateway..."
      helm upgrade --install provider-gateway "$CHART_DIR/praxis-gateway" \
        --kube-context "$CONTEXT" \
        --namespace grid-system \
        --values "$PROVIDER_VALUES" \
        "${PROVIDER_OV[@]+"${PROVIDER_OV[@]}"}" \
        "${GW_SETS[@]+"${GW_SETS[@]}"}" \
        --wait --timeout 120s
    fi

    # Consumer gateway
    CONSUMER_VALUES="${VALUES_DIR}/${SITE}-consumer-gateway.yaml"
    if [[ -f "$CONSUMER_VALUES" ]]; then
      CONSUMER_OV=()
      OV_ARGS=$(get_override_args "$SITE" "consumer")
      if [[ -n "$OV_ARGS" ]]; then
        read -ra CONSUMER_OV <<< "$OV_ARGS"
      fi

      echo "  Installing consumer-gateway..."
      helm upgrade --install consumer-gateway "$CHART_DIR/praxis-gateway" \
        --kube-context "$CONTEXT" \
        --namespace grid-system \
        --values "$CONSUMER_VALUES" \
        "${CONSUMER_OV[@]+"${CONSUMER_OV[@]}"}" \
        "${GW_SETS[@]+"${GW_SETS[@]}"}" \
        --wait --timeout 120s
    fi
  fi

  echo ""
done

echo "Installation complete. Run verify.sh to check deployment health."
