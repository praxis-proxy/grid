#!/bin/bash
# Generate the operator install manifests from charts/grid-operator.
#
# Renders the chart once per directory and writes one YAML file per object:
#   deploy/operator            the platform base: image through params.env, namespace
#                              left to the kustomize consumer, no platform assumptions
#   deploy/overlays/<flavor>   a platform flavor of the base, rendered from the base
#                              values plus the flavor's values.yaml as a delta, with the
#                              API versions in its optional api-versions file
#   deploy/overlays/kind       the standalone install xtask applies to a kind cluster,
#                              rendered from its own values.yaml alone
# With --check, writes nothing and fails if the committed files differ from what the
# chart renders.
#
# Usage:
#   ./scripts/generate-deployment-operator.sh           # regenerate every directory
#   ./scripts/generate-deployment-operator.sh --check   # verify only, for CI
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
CHART="$REPO_ROOT/charts/grid-operator"
NAMESPACE_PLACEHOLDER="grid-system"

CHECK=false
case "${1:-}" in
  "") ;;
  --check) CHECK=true ;;
  *)
    echo "usage: $0 [--check]" >&2
    exit 2
    ;;
esac

for cmd in helm yq; do
  if ! command -v "$cmd" >/dev/null 2>&1; then
    echo "error: $cmd is required but not installed" >&2
    exit 1
  fi
done

cd "$REPO_ROOT"
TMP_DIR="$(mktemp -d)"
trap 'rm -rf "$TMP_DIR"' EXIT

# Files the generator never touches in a target directory.
KEEP_FILES=(values.yaml api-versions Containerfile Containerfile.ubi9)

# The rendered objects in a directory, as kustomization resource lines, namespace first.
resource_list() {
  local dir="$1" f base
  [ -e "$dir/namespace.yaml" ] && echo "  - namespace.yaml"
  # Byte order, so the list does not depend on the generating shell's locale.
  for f in $(LC_ALL=C ls "$dir"/*.yaml | LC_ALL=C sort); do
    base="$(basename "$f")"
    case "$base" in values.yaml|namespace.yaml|kustomization.yaml) continue ;; esac
    echo "  - $base"
  done
}

# render <out-dir> <values...> -- <strip-namespace> <api-versions...>
# Values files merge in order, so a flavor's file is a delta on the base's.
# strip-namespace=yes drops metadata.namespace from every object, for a base whose
# kustomization sets the namespace and whose consumer overrides it. The kind overlay keeps
# the rendered namespaces, since one RoleBinding lives outside the operator namespace.
render() {
  local out="$1"
  shift
  local values_args=()
  while [ "$1" != "--" ]; do values_args+=(--values "$1"); shift; done
  shift
  local strip_ns="$1"
  shift
  local api_args=()
  local v
  for v in "$@"; do api_args+=(--api-versions "$v"); done
  mkdir -p "$out"
  helm template grid-operator "$CHART" \
    --namespace "$NAMESPACE_PLACEHOLDER" \
    "${values_args[@]}" \
    "${api_args[@]}" \
    --skip-tests \
    > "$out/all.yaml"
  # One file per object, named <kind>-<name>.yaml. Helm-only labels go, and so does the
  # version label: the kind kustomization sets it once, and the platform base leaves it to
  # the platform, whose image is not the chart's version. The namespace goes too, since
  # the kustomization sets it and a consumer overrides it.
  local count
  count="$(yq eval-all '[.] | length' "$out/all.yaml")"
  local i
  for ((i = 0; i < count; i++)); do
    local kind name ns file
    kind="$(yq eval-all "select(documentIndex == $i) | .kind" "$out/all.yaml")"
    name="$(yq eval-all "select(documentIndex == $i) | .metadata.name" "$out/all.yaml")"
    ns="$(yq eval-all "select(documentIndex == $i) | .metadata.namespace // \"\"" "$out/all.yaml")"
    file="$out/$(echo "$kind" | tr '[:upper:]' '[:lower:]')-${name#grid-operator-}.yaml"
    [ "$name" = "grid-operator" ] && file="$out/$(echo "$kind" | tr '[:upper:]' '[:lower:]').yaml"
    # A namespaced object outside the operator namespace keeps the namespace in its file name.
    if [ -n "$ns" ] && [ "$ns" != "$NAMESPACE_PLACEHOLDER" ]; then
      file="${file%.yaml}-$ns.yaml"
    fi
    # The base drops every namespace, including ServiceAccount subject namespaces: kustomize
    # fills an omitted subject namespace from the consumer's namespace but leaves a set one
    # alone. The overlay fills the operator namespace in on each namespaced object the chart
    # left bare, so a bare kubectl apply -k lands correctly.
    local ns_filter="."
    case "$strip_ns:$kind:$ns" in
      yes:*) ns_filter="del(.metadata.namespace) | del(.subjects[]? | select(.kind == \"ServiceAccount\" and .namespace == \"$NAMESPACE_PLACEHOLDER\") | .namespace)" ;;
      no:ClusterRole:*|no:ClusterRoleBinding:*|no:Namespace:*) ;;
      no:*:) ns_filter=".metadata.namespace = \"$NAMESPACE_PLACEHOLDER\"" ;;
    esac
    yq eval-all "select(documentIndex == $i)
      | del(.metadata.labels[\"helm.sh/chart\"])
      | del(.metadata.labels[\"app.kubernetes.io/managed-by\"])
      | $ns_filter
      | del(.. | select(tag == \"!!map\" and has(\"helm.sh/chart\")) | .[\"helm.sh/chart\"])
      | del(.. | select(tag == \"!!map\" and has(\"app.kubernetes.io/managed-by\")) | .[\"app.kubernetes.io/managed-by\"])
      | del(.. | select(tag == \"!!map\" and has(\"app.kubernetes.io/version\")) | .[\"app.kubernetes.io/version\"])" \
      "$out/all.yaml" > "$file"
  done
  rm -f "$out/all.yaml"
}

# render_base <out-dir> <values...> -- <api-versions...>
# A vendorable base: namespaces stripped, image through params.env with a replacement.
render_base() {
  local out="$1"
  shift
  local values=()
  while [ "$1" != "--" ]; do values+=("$1"); shift; done
  shift
  render "$out" "${values[@]}" -- yes "$@"
  local image_default
  image_default="$(yq '.spec.template.spec.containers[0].image' "$out/deployment.yaml")"
  cat > "$out/params.env" <<EOF
grid-operator-image=$image_default
namespace=$NAMESPACE_PLACEHOLDER
EOF
  local resources
  resources="$(resource_list "$out")"
  {
    echo "apiVersion: kustomize.config.k8s.io/v1beta1"
    echo "kind: Kustomization"
    echo ""
    echo "# Generated from charts/grid-operator by scripts/generate-deployment-operator.sh."
    echo "# Set the namespace in params.env: the replacements below carry it to every object,"
    echo "# its ServiceAccount subjects, and the ServiceMonitor. This line is only the default."
    echo "namespace: $NAMESPACE_PLACEHOLDER"
    echo ""
    echo "resources:"
    echo "$resources"
    cat <<'EOF'

labels:
  - pairs:
      app.kubernetes.io/name: grid-operator
      app.kubernetes.io/managed-by: kustomize

configMapGenerator:
  - name: grid-operator-parameters
    envs:
      - params.env

generatorOptions:
  disableNameSuffixHash: true

replacements:
  - source:
      kind: ConfigMap
      name: grid-operator-parameters
      fieldPath: data.grid-operator-image
    targets:
      - select:
          kind: Deployment
          name: grid-operator
        fieldPaths:
          - spec.template.spec.containers.[name=operator].image
  - source:
      kind: ConfigMap
      name: grid-operator-parameters
      fieldPath: data.namespace
    targets:
      - select:
          namespace: grid-system
        fieldPaths:
          - metadata.namespace
EOF
    if [ -e "$out/servicemonitor.yaml" ]; then
      cat <<'EOF'
      - select:
          kind: ServiceMonitor
          name: grid-operator
        fieldPaths:
          - spec.namespaceSelector.matchNames.0
      - select:
          kind: ServiceMonitor
          name: grid-operator
        fieldPaths:
          - spec.endpoints.0.tlsConfig.serverName
        options:
          delimiter: "."
          index: 1
EOF
    fi
  } > "$out/kustomization.yaml"
}

# The platform base, with no platform assumptions.
BASE="$TMP_DIR/operator"
render_base "$BASE" deploy/operator/values.yaml --

# Every flavor: the base values plus the flavor's delta, with its API versions.
FLAVORS=()
for dir in deploy/overlays/*/; do
  flavor="$(basename "$dir")"
  [ "$flavor" = kind ] && continue
  [ -f "$dir/values.yaml" ] || continue
  api_versions=()
  if [ -f "$dir/api-versions" ]; then
    mapfile -t api_versions < <(grep -v '^\s*#' "$dir/api-versions" | grep -v '^\s*$')
  fi
  render_base "$TMP_DIR/$flavor" deploy/operator/values.yaml "$dir/values.yaml" -- "${api_versions[@]}"
  FLAVORS+=("$flavor")
done

# The kind overlay: plain, with its own Namespace so kubectl apply -k works bare.
APP_VERSION="$(yq '.appVersion' "$CHART/Chart.yaml")"
KIND="$TMP_DIR/kind"
render "$KIND" deploy/overlays/kind/values.yaml -- no
cat > "$KIND/namespace.yaml" <<EOF
apiVersion: v1
kind: Namespace
metadata:
  name: $NAMESPACE_PLACEHOLDER
EOF
KIND_RESOURCES="$(resource_list "$KIND")"
{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo ""
  echo "# Generated from charts/grid-operator by scripts/generate-deployment-operator.sh."
  echo "# Objects carry their namespaces: one RoleBinding lives outside $NAMESPACE_PLACEHOLDER."
  echo ""
  echo "resources:"
  echo "$KIND_RESOURCES"
  cat <<EOF

labels:
  - pairs:
      app.kubernetes.io/name: grid-operator
      app.kubernetes.io/managed-by: kustomize
      app.kubernetes.io/version: "$APP_VERSION"
EOF
} > "$KIND/kustomization.yaml"

# sync <generated-dir> <target-dir>
sync_dir() {
  local src="$1" dst="$2"
  mkdir -p "$dst"
  local f base
  for f in "$dst"/*; do
    [ -e "$f" ] || continue
    base="$(basename "$f")"
    case " ${KEEP_FILES[*]} " in *" $base "*) continue ;; esac
    if [ ! -e "$src/$base" ]; then
      if $CHECK; then
        echo "error: $dst/$base is not generated by the chart; run make generate-deploy" >&2
        exit 1
      fi
      rm -f "$f"
    fi
  done
  for f in "$src"/*; do
    base="$(basename "$f")"
    if $CHECK; then
      if ! cmp -s "$f" "$dst/$base"; then
        echo "error: $dst/$base is out of date; run make generate-deploy" >&2
        diff -u "$dst/$base" "$f" >&2 || true
        exit 1
      fi
    else
      cp "$f" "$dst/$base"
    fi
  done
}

sync_dir "$BASE" "$REPO_ROOT/deploy/operator"
for flavor in "${FLAVORS[@]}"; do
  sync_dir "$TMP_DIR/$flavor" "$REPO_ROOT/deploy/overlays/$flavor"
done
sync_dir "$KIND" "$REPO_ROOT/deploy/overlays/kind"

DIRS="deploy/operator, deploy/overlays/kind"
for flavor in "${FLAVORS[@]}"; do DIRS="$DIRS, deploy/overlays/$flavor"; done
if $CHECK; then
  echo "$DIRS match the chart"
else
  echo "wrote $DIRS"
fi
