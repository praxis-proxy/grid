#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Renders the chart for every values file under tests/values and diffs the output
# against tests/golden/<case>.yaml. UPDATE_GOLDEN=1 rewrites the golden files.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
CHART="$ROOT/charts/grid-fleet-dashboard"
VALUES_DIR="$CHART/tests/values"
GOLDEN_DIR="$CHART/tests/golden"
RELEASE=grid-fleet-dashboard
NAMESPACE=aigrid-fleet

command -v helm >/dev/null || { echo "helm not found in PATH" >&2; exit 1; }
mkdir -p "$GOLDEN_DIR"

rc=0
for values_file in "$VALUES_DIR"/*.yaml; do
  case=$(basename "$values_file" .yaml)
  golden="$GOLDEN_DIR/$case.yaml"
  rendered=$(helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" --values "$values_file")
  if [[ "${UPDATE_GOLDEN:-0}" == "1" ]]; then
    printf '%s\n' "$rendered" > "$golden"
    echo "updated $golden"
    continue
  fi
  if [[ ! -f "$golden" ]]; then
    echo "FAIL $case: missing $golden (run UPDATE_GOLDEN=1 $0)" >&2
    rc=1
    continue
  fi
  if diff -u "$golden" <(printf '%s\n' "$rendered"); then
    echo "ok   $case"
  else
    echo "FAIL $case: rendered output differs from $golden" >&2
    rc=1
  fi
done
exit $rc
