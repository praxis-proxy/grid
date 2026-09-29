#!/bin/bash
# Generate the Grid CRD manifests from the Rust types in operator/src/crd.
#
# Writes one YAML file per CRD to deploy/crds and charts/grid-operator/crds.
# Also writes deploy/crds/kustomization.yaml listing those files.
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
OUT_DIRS=("$CRD_DIR" "$REPO_ROOT/charts/grid-operator/crds")

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
count="$(jq '.items | length' "$TMP_DIR/crds.json")"
for ((i = 0; i < count; i++)); do
  name="$(jq -r ".items[$i].spec.names.singular" "$TMP_DIR/crds.json")"
  jq ".items[$i]" "$TMP_DIR/crds.json" | yq eval -P - > "$TMP_DIR/$name.yaml"
done

if [ "$CHECK" = true ]; then
  status=0
  for dir in "${OUT_DIRS[@]}"; do
    for generated in "$TMP_DIR"/*.yaml; do
      committed="$dir/$(basename "$generated")"
      if [ ! -f "$committed" ]; then
        echo "error: ${committed#"$REPO_ROOT"/} is missing. Run ./scripts/generate-deployment-crds.sh." >&2
        status=1
      elif ! diff -u "$committed" "$generated"; then
        echo "error: ${committed#"$REPO_ROOT"/} does not match the Rust CRD types. Run ./scripts/generate-deployment-crds.sh." >&2
        status=1
      fi
    done
    for committed in "$dir"/*.yaml; do
      [ "$(basename "$committed")" = kustomization.yaml ] && continue
      if [ ! -f "$TMP_DIR/$(basename "$committed")" ]; then
        echo "error: ${committed#"$REPO_ROOT"/} has no matching Rust CRD type. Remove it or add the type to generate_crds." >&2
        status=1
      fi
    done
  done
  if [ "$status" -eq 0 ]; then
    echo "CRD manifests match the Rust CRD types."
  fi
  exit "$status"
fi

for dir in "${OUT_DIRS[@]}"; do
  mkdir -p "$dir"
  cp "$TMP_DIR"/*.yaml "$dir/"
  echo "Wrote CRDs to ${dir#"$REPO_ROOT"/}:"
  (cd "$dir" && ls -1 ./*.yaml)
done

# Kustomization over the generated CRDs, for kubectl apply -k and kustomize consumers.
# Only deploy/crds gets one: Helm would try to install it from the chart's crds/.
{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo "resources:"
  for f in "$TMP_DIR"/*.yaml; do
    echo "  - $(basename "$f")"
  done
} > "$CRD_DIR/kustomization.yaml"

echo ""
echo "To validate CRDs:"
echo "  kubectl --dry-run=server create -k deploy/crds/"
