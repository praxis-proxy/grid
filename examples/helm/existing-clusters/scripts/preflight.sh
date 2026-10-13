#!/usr/bin/env bash
# Preflight checks for existing-cluster Grid installation.
# Validates cluster access, permissions, image availability, and connectivity
# before making any changes.
#
# Usage: ./preflight.sh <inventory.yaml>
#
# Requires: kubectl, helm, jq, python3, mikefarah/yq >= 4.18.0, openssl (optional, for SAN check)

set -euo pipefail

INVENTORY="${1:?Usage: $0 <inventory.yaml>}"

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory file not found: $INVENTORY" >&2
  exit 1
fi

for cmd in kubectl helm yq jq python3; do
  if ! command -v "$cmd" &>/dev/null; then
    echo "ERROR: required command not found: $cmd" >&2
    case "$cmd" in
      yq)      echo "  Install: https://github.com/mikefarah/yq#install (requires >= 4.18.0)" >&2 ;;
      jq)      echo "  Install: https://jqlang.github.io/jq/download/" >&2 ;;
      python3) echo "  Install: https://www.python.org/downloads/ or your OS package manager" >&2 ;;
    esac
    exit 1
  fi
done

YQ_VERSION_OUTPUT=$(yq --version 2>&1)
if [[ "$YQ_VERSION_OUTPUT" =~ version[[:space:]]+v?([0-9]+)\.([0-9]+)\.([0-9]+) ]]; then
  YQ_MAJOR=${BASH_REMATCH[1]}
  YQ_MINOR=${BASH_REMATCH[2]}
  YQ_PATCH=${BASH_REMATCH[3]}
else
  echo "ERROR: unsupported yq implementation; install mikefarah/yq >= 4.18.0" >&2
  exit 1
fi

if ((YQ_MAJOR < 4 || (YQ_MAJOR == 4 && YQ_MINOR < 18))); then
  echo "ERROR: mikefarah/yq >= 4.18.0 required, found ${YQ_MAJOR}.${YQ_MINOR}.${YQ_PATCH}" >&2
  exit 1
fi

TOPOLOGY=$(yq eval '.topology' "$INVENTORY")
if [[ "$TOPOLOGY" != "dedicated-edge" && "$TOPOLOGY" != "combined-site" ]]; then
  echo "ERROR: inventory topology must be 'dedicated-edge' or 'combined-site', got: $TOPOLOGY" >&2
  exit 1
fi

ERRORS=0

check() {
  local desc="$1"
  shift
  if "$@" &>/dev/null; then
    echo "  PASS  $desc"
  else
    echo "  FAIL  $desc" >&2
    ERRORS=$((ERRORS + 1))
  fi
}

OP_REPO=$(yq eval '.images.operator.repository // ""' "$INVENTORY")
OP_TAG=$(yq eval '.images.operator.tag // ""' "$INVENTORY")
if [[ -n "$OP_TAG" ]]; then
  case "$OP_TAG" in
    v0.0.*|v0.1.0)
      echo "FAIL  operator image ${OP_REPO}:${OP_TAG} lacks health endpoints (requires v0.1.1+)" >&2
      ERRORS=$((ERRORS + 1))
      ;;
  esac
fi

SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")

echo "Preflight: topology=$TOPOLOGY"
echo ""

CHART_DIR="$(cd "$(dirname "$0")/../../../.." && pwd)/charts"
VALUES_DIR="$(cd "$(dirname "$0")/.." && pwd)/${TOPOLOGY}/values"

CUSTOM_VALUES_DIR=$(yq eval '.valuesDir // ""' "$INVENTORY")
if [[ -n "$CUSTOM_VALUES_DIR" ]]; then
  if [[ ! -d "$CUSTOM_VALUES_DIR" ]]; then
    echo "  WARN  valuesDir not found: $CUSTOM_VALUES_DIR" >&2
  else
    VALUES_DIR="$CUSTOM_VALUES_DIR"
  fi
fi

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  echo "Site: $SITE (context: $CONTEXT)"

  SITE_COUNT=$(echo "$SITE_NAMES" | wc -w)

  GW_ADDR=$(yq eval ".sites.${SITE}.gatewayAddress // \"\"" "$INVENTORY")
  if [[ -z "$GW_ADDR" || "$GW_ADDR" == "replace-me" ]]; then
    if [[ "$SITE_COUNT" -gt 1 ]]; then
      echo "  FAIL  gatewayAddress not configured for $SITE" >&2
      ERRORS=$((ERRORS + 1))
    else
      echo "  SKIP  gatewayAddress (single-site, no cross-cluster routing)"
    fi
  else
    echo "  PASS  gatewayAddress: $GW_ADDR"
  fi

  SWIM_ADDR=$(yq eval ".sites.${SITE}.swimAddress // \"\"" "$INVENTORY")
  if [[ -z "$SWIM_ADDR" || "$SWIM_ADDR" == "replace-me" ]]; then
    if [[ "$SITE_COUNT" -gt 1 ]]; then
      echo "  FAIL  swimAddress not configured for $SITE" >&2
      ERRORS=$((ERRORS + 1))
    else
      echo "  SKIP  swimAddress (single-site, no SWIM peers)"
    fi
  else
    echo "  PASS  swimAddress: $SWIM_ADDR"
  fi

  check "cluster reachable" kubectl --context "$CONTEXT" cluster-info
  check "namespace create permission" kubectl --context "$CONTEXT" auth can-i create namespaces
  check "deployment create permission" kubectl --context "$CONTEXT" auth can-i create deployments -n grid-system
  check "secret create permission" kubectl --context "$CONTEXT" auth can-i create secrets -n grid-system
  check "service create permission" kubectl --context "$CONTEXT" auth can-i create services -n grid-system
  check "configmap create permission" kubectl --context "$CONTEXT" auth can-i create configmaps -n grid-system

  GW_REPO=$(yq eval '.images.gateway.repository // ""' "$INVENTORY")
  GW_TAG=$(yq eval '.images.gateway.tag // ""' "$INVENTORY")
  if [[ -n "$GW_REPO" && -n "$GW_TAG" ]]; then
    GATEWAY_IMAGE="${GW_REPO}:${GW_TAG}"
    check "gateway image pullable" kubectl --context "$CONTEXT" run preflight-pull-test \
      --image="$GATEWAY_IMAGE" --restart=Never --rm -i --command -- echo ok
  else
    echo "  SKIP  gateway image pull (image specified in per-site values)"
  fi

  if [[ -n "$OP_REPO" && -n "$OP_TAG" && "$OP_REPO" != "ghcr.io/praxis-proxy/grid-operator" ]]; then
    OPERATOR_IMAGE="${OP_REPO}:${OP_TAG}"
    check "operator image pullable" kubectl --context "$CONTEXT" run preflight-op-pull-test \
      --image="$OPERATOR_IMAGE" --restart=Never --rm -i --command -- echo ok
  else
    echo "  SKIP  operator image pull (image specified in per-site values)"
  fi

  KUBE_VERSION=$(kubectl --context "$CONTEXT" version -o json 2>/dev/null \
    | python3 -c "import json,sys; v=json.load(sys.stdin).get('serverVersion',{}); print(v.get('major','0')+'.'+v.get('minor','0').rstrip('+'))" 2>/dev/null || echo "0.0")
  MAJOR=$(echo "$KUBE_VERSION" | cut -d. -f1)
  MINOR=$(echo "$KUBE_VERSION" | cut -d. -f2)
  if [[ "$MAJOR" -ge 1 && "$MINOR" -ge 27 ]]; then
    echo "  PASS  Kubernetes version >= 1.27 ($KUBE_VERSION)"
  else
    echo "  FAIL  Kubernetes version >= 1.27 (got $KUBE_VERSION)" >&2
    ERRORS=$((ERRORS + 1))
  fi

  # Prerequisite resources

  if ! kubectl --context "$CONTEXT" get namespace grid-system &>/dev/null; then
    echo "  FAIL  grid-system namespace does not exist; create it and populate prerequisite resources before installing" >&2
    ERRORS=$((ERRORS + 1))
  else
    ROLES=$(yq eval ".sites.${SITE}.roles[]" "$INVENTORY" 2>/dev/null || echo "")
    HAS_CONSUMER=false
    HAS_PROVIDER=false
    if [[ "$TOPOLOGY" == "combined-site" ]]; then
      HAS_CONSUMER=true
      HAS_PROVIDER=true
    elif echo "$ROLES" | grep -q "consumer"; then
      HAS_CONSUMER=true
    elif echo "$ROLES" | grep -q "provider"; then
      HAS_PROVIDER=true
    fi

    if $HAS_CONSUMER; then
      if [[ -f "${VALUES_DIR}/${SITE}-consumer-praxis.yaml" ]]; then
        echo "  SKIP  consumer-praxis-config ConfigMap (template found, install.sh creates it)"
      else
        check "consumer-praxis-config ConfigMap" kubectl --context "$CONTEXT" -n grid-system \
          get configmap consumer-praxis-config
      fi
      check "consumer-tls Secret" kubectl --context "$CONTEXT" -n grid-system \
        get secret consumer-tls
    fi

    if $HAS_PROVIDER; then
      if [[ -f "${VALUES_DIR}/${SITE}-provider-praxis.yaml" ]]; then
        echo "  SKIP  provider-praxis-config ConfigMap (template found, install.sh renders from overlay)"
      else
        check "provider-praxis-config ConfigMap" kubectl --context "$CONTEXT" -n grid-system \
          get configmap provider-praxis-config
      fi
      check "provider-tls Secret" kubectl --context "$CONTEXT" -n grid-system \
        get secret provider-tls
    fi

    # Credential Secret validation

    MOCK_VALUES="${VALUES_DIR}/${SITE}-grid-mock-providers.yaml"
    if [[ -f "$MOCK_VALUES" ]]; then
      CRED_SECRETS=$(yq eval '.providers[].credentialSecret' "$MOCK_VALUES" 2>/dev/null || echo "")
      CRED_KEYS=$(yq eval '.providers[].credentialKey' "$MOCK_VALUES" 2>/dev/null || echo "")
      while IFS=$'\t' read -r SEC_NAME SEC_KEY; do
        [[ -z "$SEC_NAME" || "$SEC_NAME" == "null" ]] && continue
        [[ -z "$SEC_KEY" ]] && SEC_KEY="token"
        if ! kubectl --context "$CONTEXT" -n grid-system get secret "$SEC_NAME" &>/dev/null; then
          echo "  FAIL  credential Secret '$SEC_NAME' not found" >&2
          ERRORS=$((ERRORS + 1))
          continue
        fi
        CRED_B64=$(kubectl --context "$CONTEXT" -n grid-system \
          get secret "$SEC_NAME" -o jsonpath="{.data.${SEC_KEY}}" 2>/dev/null || echo "")
        if [[ -z "$CRED_B64" ]]; then
          echo "  FAIL  credential Secret '$SEC_NAME' key '$SEC_KEY' is empty" >&2
          ERRORS=$((ERRORS + 1))
          continue
        fi
        CRED_RAW=$(echo "$CRED_B64" | base64 -d 2>/dev/null || echo "")
        if [[ -z "$CRED_RAW" ]]; then
          echo "  FAIL  credential Secret '$SEC_NAME' key '$SEC_KEY' decoded to empty" >&2
          ERRORS=$((ERRORS + 1))
        elif [[ "$CRED_RAW" == *$'\n' || "$CRED_RAW" == *$'\r' ]]; then
          echo "  FAIL  credential Secret '$SEC_NAME' key '$SEC_KEY' has trailing newline; mock backends will reject the token; recreate with: printf '%s' \"\$(cat tokenfile)\" | kubectl create secret generic $SEC_NAME --from-file=${SEC_KEY}=/dev/stdin --dry-run=client -o yaml | kubectl apply -f -" >&2
          ERRORS=$((ERRORS + 1))
        else
          echo "  PASS  credential Secret '$SEC_NAME' key '$SEC_KEY' present (no trailing newline)"
        fi
      done < <(paste <(echo "$CRED_SECRETS") <(echo "$CRED_KEYS"))
    fi

    # GridSite provider-site label

    if $HAS_PROVIDER; then
      if [[ -f "${VALUES_DIR}/${SITE}-grid-site.yaml" ]]; then
        echo "  SKIP  GridSite label (grid-site chart will create it)"
      else
        GRIDSITE_WITH_LABEL=$(kubectl --context "$CONTEXT" -n grid-system \
          get gridsite -l "grid.praxis.fast/provider-site" \
          -o name 2>/dev/null || echo "")
        if [[ -n "$GRIDSITE_WITH_LABEL" ]]; then
          echo "  PASS  GridSite has provider-site label"
        else
          echo "  FAIL  no GridSite with grid.praxis.fast/provider-site label; InferenceProvider siteSelector will not match" >&2
          ERRORS=$((ERRORS + 1))
        fi
      fi
    fi

    # TLS SAN inspection

    if command -v openssl &>/dev/null; then
      inspect_tls_sans() {
        local secret_name="$1" context="$2"
        local cert_b64
        cert_b64=$(kubectl --context "$context" -n grid-system \
          get secret "$secret_name" -o jsonpath='{.data.tls\.crt}' 2>/dev/null || echo "")
        if [[ -z "$cert_b64" ]]; then
          echo "  WARN  ${secret_name}: no tls.crt data (verify manually)" >&2
          return
        fi
        local sans
        sans=$(echo "$cert_b64" | base64 -d \
          | openssl x509 -noout -ext subjectAltName 2>/dev/null \
          | grep -oP 'DNS:[^ ,]+' | tr '\n' ' ' || echo "")
        if [[ -n "$sans" ]]; then
          echo "  INFO  ${secret_name} SANs: ${sans}"
        else
          echo "  WARN  ${secret_name}: no DNS SANs found" >&2
        fi
      }

      if $HAS_CONSUMER; then
        inspect_tls_sans "consumer-tls" "$CONTEXT"
      fi
      if $HAS_PROVIDER; then
        inspect_tls_sans "provider-tls" "$CONTEXT"
      fi
    fi

    # Helm chart renderability

    if [[ -d "$CHART_DIR/grid-operator" ]]; then
      OPERATOR_VALUES="${VALUES_DIR}/${SITE}-operator.yaml"
      if [[ -f "$OPERATOR_VALUES" ]]; then
        if helm template grid-operator "$CHART_DIR/grid-operator" \
          --namespace grid-system \
          --values "$OPERATOR_VALUES" \
          "${IMAGE_SETS[@]+"${IMAGE_SETS[@]}"}" \
          &>/dev/null; then
          echo "  PASS  operator chart renders"
        else
          echo "  FAIL  operator chart fails to render with $OPERATOR_VALUES" >&2
          ERRORS=$((ERRORS + 1))
        fi
      fi

      if [[ "$TOPOLOGY" == "combined-site" ]]; then
        for GW_ROLE in consumer provider; do
          GW_VALUES="${VALUES_DIR}/${SITE}-${GW_ROLE}-gateway.yaml"
          if [[ -f "$GW_VALUES" ]]; then
            if helm template "${GW_ROLE}-gateway" "$CHART_DIR/praxis-gateway" \
              --namespace grid-system \
              --values "$GW_VALUES" \
              &>/dev/null; then
              echo "  PASS  ${GW_ROLE}-gateway chart renders"
            else
              echo "  FAIL  ${GW_ROLE}-gateway chart fails to render with $GW_VALUES" >&2
              ERRORS=$((ERRORS + 1))
            fi
          fi
        done
      elif [[ "$TOPOLOGY" == "dedicated-edge" ]]; then
        GW_VALUES="${VALUES_DIR}/${SITE}-gateway.yaml"
        if [[ -f "$GW_VALUES" ]]; then
          if helm template gateway "$CHART_DIR/praxis-gateway" \
            --namespace grid-system \
            --values "$GW_VALUES" \
            &>/dev/null; then
            echo "  PASS  gateway chart renders"
          else
            echo "  FAIL  gateway chart fails to render with $GW_VALUES" >&2
            ERRORS=$((ERRORS + 1))
          fi
        fi
      fi
    fi
  fi

  echo ""
done

SEEN_NAMES=""
for SITE in $SITE_NAMES; do
  if echo "$SEEN_NAMES" | grep -qw "$SITE"; then
    echo "  FAIL  duplicate site name: $SITE" >&2
    ERRORS=$((ERRORS + 1))
  fi
  SEEN_NAMES="$SEEN_NAMES $SITE"
done

if [[ "$ERRORS" -gt 0 ]]; then
  echo "Preflight FAILED with $ERRORS error(s). Fix issues before running install.sh." >&2
  exit 1
fi

echo "Preflight PASSED. All $( echo "$SITE_NAMES" | wc -w) sites ready."
