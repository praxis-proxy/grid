#!/bin/bash
# Generate Grid CRDs for deployment manifests

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CRD_DIR="$REPO_ROOT/deploy/crds"

cd "$REPO_ROOT"

echo "Generating Grid CRDs..."
mkdir -p "$CRD_DIR"

# Generate CRDs and split into individual YAML files
cargo run -p operator --bin generate_crds | jq -r '.items[0]' | yq eval -P > "$CRD_DIR/gridnetwork.yaml"
cargo run -p operator --bin generate_crds | jq -r '.items[1]' | yq eval -P > "$CRD_DIR/gridsite.yaml"
cargo run -p operator --bin generate_crds | jq -r '.items[2]' | yq eval -P > "$CRD_DIR/inferenceprovider.yaml"

echo "CRDs generated in $CRD_DIR:"
ls -la "$CRD_DIR"

echo ""
echo "To validate CRDs:"
echo "  kubectl --dry-run=server create -f deploy/crds/"
echo ""
echo "To regenerate after schema changes:"
echo "  ./scripts/generate-deployment-crds.sh"
