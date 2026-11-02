#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Renders the chart for every values file under tests/values and diffs the output
# against tests/golden/<case>.yaml. UPDATE_GOLDEN=1 rewrites the golden files.
# Then checks that the chart is on the Grid release line and that the dashboard
# image reference resolves to the chart appVersion by default and to image.tag
# when it is set.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPO_ROOT=$(cd "$ROOT/../.." && pwd)
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

[[ "${UPDATE_GOLDEN:-0}" == "1" ]] && exit $rc

# The release workflow publishes ghcr.io/praxis-proxy/grid-fleet-dashboard only
# at Grid release tags, so the chart must track the workspace version.
chart_field() { grep "^$1:" "$CHART/Chart.yaml" | awk '{print $2}' | tr -d '"'; }
CHART_VERSION=$(chart_field version)
APP_VERSION=$(chart_field appVersion)
WORKSPACE_VERSION=$(perl -ne 'print $1 and exit if /^version\s*=\s*"(.+)"/' "$REPO_ROOT/Cargo.toml")
if [[ "$CHART_VERSION" == "$WORKSPACE_VERSION" && "$APP_VERSION" == "v$WORKSPACE_VERSION" ]]; then
  echo "ok   chart version $CHART_VERSION / appVersion $APP_VERSION match workspace $WORKSPACE_VERSION"
else
  echo "FAIL chart version $CHART_VERSION / appVersion $APP_VERSION, expected $WORKSPACE_VERSION / v$WORKSPACE_VERSION (Cargo.toml)" >&2
  rc=1
fi

# expect_image <label> <expected image> <helm template args...>
expect_image() {
  local label="$1" want="$2" got
  shift 2
  got=$(helm template "$RELEASE" "$CHART" --namespace "$NAMESPACE" "$@" |
    awk '/- name: dashboard$/{f=1} f && /image:/{gsub(/"/, "", $2); print $2; exit}')
  if [[ "$got" == "$want" ]]; then
    echo "ok   $label: $got"
  else
    echo "FAIL $label: dashboard image is '$got', expected '$want'" >&2
    rc=1
  fi
}
REPOSITORY=ghcr.io/praxis-proxy/grid-fleet-dashboard
expect_image "default image" "$REPOSITORY:$APP_VERSION"
expect_image "image.tag override" "$REPOSITORY:v0.1.0-override" --set image.tag=v0.1.0-override
expect_image "image.repository and image.tag override" "registry.example.com/fleet:dev" \
  --set image.repository=registry.example.com/fleet --set image.tag=dev
exit $rc
