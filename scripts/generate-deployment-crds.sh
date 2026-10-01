#!/bin/bash
# Generate the Grid CRD manifests from the Rust types in operator/src/crd.
#
# Writes one YAML file per CRD to deploy/crds, its kustomization.yaml, and a
# copy gated by crds.enabled to charts/grid-operator/templates/crds.
# With --check, writes nothing and fails if the committed files differ from
# what the Rust types generate.
#
# Usage:
#   ./scripts/generate-deployment-crds.sh           # regenerate the CRD files
#   ./scripts/generate-deployment-crds.sh --check   # verify only, for CI

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CRD_DIR="$REPO_ROOT/deploy/crds"
CHART_CRD_DIR="$REPO_ROOT/charts/grid-operator/templates/crds"

CHECK=false
case "${1:-}" in
  "") ;;
  --check) CHECK=true ;;
  *)
    echo "usage: $0 [--check]" >&2
    exit 2
    ;;
esac

for cmd in cargo jq yq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: $cmd is required but not installed" >&2
    exit 1
  fi
done

cd "$REPO_ROOT"

TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

cargo run --quiet -p operator --bin generate_crds > "$TMP_DIR/crds.json"

# One file per CRD, named after its singular name, for example gridnetwork.yaml.
mkdir -p "$TMP_DIR/deploy" "$TMP_DIR/chart"
count="$(jq '.items | length' "$TMP_DIR/crds.json")"
for ((i = 0; i < count; i++)); do
  name="$(jq -r ".items[$i].spec.names.singular" "$TMP_DIR/crds.json")"
  jq ".items[$i]" "$TMP_DIR/crds.json" | yq eval -P - > "$TMP_DIR/deploy/$name.yaml"
  # crds.keep keeps them through helm uninstall and Argo CD, which ignores the Helm policy.
  {
    echo '{{- if .Values.crds.enabled }}'
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
cp "$TMP_DIR/kustomization.yaml" "$TMP_DIR/deploy/"

# check_dir <generated dir> <committed dir>
check_dir() {
  local status=0 generated committed
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
    if [ ! -f "$1/$(basename "$committed")" ]; then
      echo "error: ${committed#"$REPO_ROOT"/} has no matching Rust CRD type. Remove it or add the type to generate_crds." >&2
      status=1
    fi
  done
  return "$status"
}

if [ "$CHECK" = true ]; then
  status=0
  check_dir "$TMP_DIR/deploy" "$CRD_DIR" || status=1
  check_dir "$TMP_DIR/chart" "$CHART_CRD_DIR" || status=1
  if [ "$status" -eq 0 ]; then
    echo "CRD manifests match the Rust CRD types."
  fi
  exit "$status"
fi

mkdir -p "$CRD_DIR" "$CHART_CRD_DIR"
rm -f "$CRD_DIR"/*.yaml "$CHART_CRD_DIR"/*.yaml
cp "$TMP_DIR"/deploy/*.yaml "$CRD_DIR/"
cp "$TMP_DIR"/chart/*.yaml "$CHART_CRD_DIR/"
echo "Wrote CRDs to ${CRD_DIR#"$REPO_ROOT"/} and ${CHART_CRD_DIR#"$REPO_ROOT"/}"

echo ""
echo "To validate CRDs:"
echo "  kubectl --dry-run=server create -k deploy/crds/"
