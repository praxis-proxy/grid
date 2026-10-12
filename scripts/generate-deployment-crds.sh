#!/bin/bash
# Generate the Grid CRD manifests from the Rust types in crates/operator/src/crd.
#
# Writes one YAML file per CRD to deploy/crds, its kustomization.yaml, and a
# copy gated by crds.enabled to charts/grid-operator/templates/crds.
# With --check, writes nothing and fails if the committed files differ from
# what the Rust types generate.
#
# Usage:
#   ./scripts/generate-deployment-crds.sh                    # stable CRDs
#   ./scripts/generate-deployment-crds.sh --check            # verify stable CRDs
#   ./scripts/generate-deployment-crds.sh --experimental     # A2A CRD
#   ./scripts/generate-deployment-crds.sh --experimental --check

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CRD_DIR="$REPO_ROOT/deploy/crds"
CHART_CRD_DIR="$REPO_ROOT/charts/grid-operator/templates/crds"

CHECK=false
EXPERIMENTAL=false
for arg in "$@"; do
  case "$arg" in
    --check) CHECK=true ;;
    --experimental) EXPERIMENTAL=true ;;
    *)
      echo "usage: $0 [--check] [--experimental]" >&2
      exit 2
      ;;
  esac
done

EXPERIMENTAL_CRD_DIR="$REPO_ROOT/deploy/crds-experimental"

for cmd in cargo jq yq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: $cmd is required but not installed" >&2
    exit 1
  fi
done

cd "$REPO_ROOT"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

FEATURE_ARGS=()
if [ "$EXPERIMENTAL" = true ]; then
  FEATURE_ARGS+=(--features a2a)
fi
cargo run --quiet -p operator "${FEATURE_ARGS[@]}" --bin generate_crds > "$TMP_DIR/crds.json"

# One file per CRD, named after its singular name, for example gridnetwork.yaml.
mkdir -p "$TMP_DIR/deploy" "$TMP_DIR/chart"
count="$(jq '.items | length' "$TMP_DIR/crds.json")"
for ((i = 0; i < count; i++)); do
  name="$(jq -r ".items[$i].spec.names.singular" "$TMP_DIR/crds.json")"
  if [ "$EXPERIMENTAL" = true ] && [ "$name" != "agenttoagentprovider" ]; then
    continue
  fi
  if [ "$EXPERIMENTAL" = false ] && [ "$name" = "agenttoagentprovider" ]; then
    continue
  fi
  jq ".items[$i]" "$TMP_DIR/crds.json" | yq eval -P - > "$TMP_DIR/deploy/$name.yaml"
  # crds.keep keeps them through helm uninstall and Argo CD, which ignores the Helm policy.
  {
    if [ "$EXPERIMENTAL" = true ]; then
      echo '{{- $experimental := .Values.experimental | default dict -}}'
      echo '{{- $a2aEnabled := dig "a2a" "enabled" false $experimental -}}'
      echo '{{- if and .Values.crds.enabled $a2aEnabled }}'
    else
      echo '{{- if .Values.crds.enabled }}'
    fi
    yq eval '.metadata.annotations["KEEP"] = "KEEP"' "$TMP_DIR/deploy/$name.yaml" \
      | sed 's|^    KEEP: KEEP$|    {{- if .Values.crds.keep }}\n    helm.sh/resource-policy: keep\n    argocd.argoproj.io/sync-options: Prune=false,Delete=false,ServerSideApply=true\n    {{- else }}\n    argocd.argoproj.io/sync-options: ServerSideApply=true\n    {{- end }}|'
    echo '{{- end }}'
  } > "$TMP_DIR/chart/$name.yaml"
  if grep -q 'KEEP: KEEP' "$TMP_DIR/chart/$name.yaml"; then
    echo "error: the KEEP marker in $name.yaml was not replaced" >&2
    exit 1
  fi
done

# Kustomization over the generated CRDs, for kubectl apply -k and kustomize consumers.
{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo "resources:"
  for f in "$TMP_DIR"/deploy/*.yaml; do
    echo "  - $(basename "$f")"
  done
} > "$TMP_DIR/kustomization.yaml"
if [ "$EXPERIMENTAL" = true ]; then
  mkdir -p "$TMP_DIR/experimental-deploy"
  cp "$TMP_DIR/deploy/agenttoagentprovider.yaml" "$TMP_DIR/experimental-deploy/"
  cp "$TMP_DIR/kustomization.yaml" "$TMP_DIR/experimental-deploy/"
else
  cp "$TMP_DIR/kustomization.yaml" "$TMP_DIR/deploy/"
fi

# check_dir <generated dir> <committed dir>
check_dir() {
  local status=0 generated committed skip_a2a="${3:-false}"
  for generated in "$1"/*.yaml; do
    committed="$2/$(basename "$generated")"
    if [ ! -f "$committed" ]; then
      echo "error: ${committed#"$REPO_ROOT"/} is missing. Run ./scripts/generate-deployment-crds.sh." >&2
      status=1
    elif ! diff -u "$committed" "$generated"; then
      echo "error: ${committed#"$REPO_ROOT"/} does not match the Rust CRD types. Run ./scripts/generate-deployment-crds.sh." >&2
      status=1
    fi
  done
  for committed in "$2"/*.yaml; do
    if [ "$skip_a2a" = true ] && [ "$(basename "$committed")" = "agenttoagentprovider.yaml" ]; then
      continue
    fi
    if [ ! -f "$1/$(basename "$committed")" ]; then
      echo "error: ${committed#"$REPO_ROOT"/} has no matching Rust CRD type. Remove it or add the type to generate_crds." >&2
      status=1
    fi
  done
  return "$status"
}

if [ "$CHECK" = true ]; then
  status=0
  if [ "$EXPERIMENTAL" = true ]; then
    check_dir "$TMP_DIR/experimental-deploy" "$EXPERIMENTAL_CRD_DIR" || status=1
    if ! diff -u "$CHART_CRD_DIR/agenttoagentprovider.yaml" "$TMP_DIR/chart/agenttoagentprovider.yaml"; then
      echo "error: chart A2A CRD does not match the experimental Rust type. Run ./scripts/generate-deployment-crds.sh --experimental." >&2
      status=1
    fi
  else
    check_dir "$TMP_DIR/deploy" "$CRD_DIR" || status=1
    check_dir "$TMP_DIR/chart" "$CHART_CRD_DIR" true || status=1
  fi
  if [ "$status" -eq 0 ]; then
    if [ "$EXPERIMENTAL" = true ]; then
      echo "Experimental A2A CRD manifests match the Rust CRD type."
    else
      echo "Stable CRD manifests match the Rust CRD types."
    fi
  fi
  exit "$status"
fi

if [ "$EXPERIMENTAL" = true ]; then
  mkdir -p "$EXPERIMENTAL_CRD_DIR" "$CHART_CRD_DIR"
  rm -f "$EXPERIMENTAL_CRD_DIR/agenttoagentprovider.yaml" "$EXPERIMENTAL_CRD_DIR/kustomization.yaml"
  cp "$TMP_DIR"/experimental-deploy/*.yaml "$EXPERIMENTAL_CRD_DIR/"
  cp "$TMP_DIR/chart/agenttoagentprovider.yaml" "$CHART_CRD_DIR/"
  echo "Wrote experimental A2A CRD to ${EXPERIMENTAL_CRD_DIR#"$REPO_ROOT"/} and ${CHART_CRD_DIR#"$REPO_ROOT"/}"
else
  mkdir -p "$CRD_DIR" "$CHART_CRD_DIR"
  # The A2A manifest belongs only in the experimental bundle. Remove a stale
  # copy here so the default generator restores the default install contract.
  rm -f "$CRD_DIR/agenttoagentprovider.yaml"
  for generated in "$TMP_DIR"/deploy/*.yaml; do
    rm -f "$CRD_DIR/$(basename "$generated")"
  done
  for generated in "$TMP_DIR"/chart/*.yaml; do
    rm -f "$CHART_CRD_DIR/$(basename "$generated")"
  done
  cp "$TMP_DIR"/deploy/*.yaml "$CRD_DIR/"
  cp "$TMP_DIR"/chart/*.yaml "$CHART_CRD_DIR/"
  echo "Wrote stable CRDs to ${CRD_DIR#"$REPO_ROOT"/} and ${CHART_CRD_DIR#"$REPO_ROOT"/}"
fi

echo ""
echo "To validate CRDs:"
if [ "$EXPERIMENTAL" = true ]; then
  echo "  kubectl --dry-run=server create -k deploy/crds-experimental/"
else
  echo "  kubectl --dry-run=server create -k deploy/crds/"
fi
