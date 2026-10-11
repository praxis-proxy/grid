#!/bin/bash
# Generate the operator install manifests from charts/grid-operator.
#
# Renders the chart twice and writes one YAML file per object:
#   deploy/operator        the platform base: OpenShift, image through params.env,
#                          namespace left to the kustomize consumer
#   deploy/overlays/kind   the standalone install xtask applies to a kind cluster
# Each directory keeps its values.yaml as the render input. With --check, writes
# nothing and fails if the committed files differ from what the chart renders.
#
# Usage:
#   ./scripts/generate-deployment-operator.sh           # regenerate both directories
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
KEEP_FILES=(values.yaml Containerfile Containerfile.ubi9)

# The rendered objects in a directory, as kustomization resource lines, namespace first.
resource_list() {
  local dir="$1" f base
  [ -e "$dir/namespace.yaml" ] && echo "  - namespace.yaml"
  for f in "$dir"/*.yaml; do
    base="$(basename "$f")"
    case "$base" in values.yaml|namespace.yaml|kustomization.yaml) continue ;; esac
    echo "  - $base"
  done
}

# render <out-dir> <values> <strip-namespace> <api-versions...>
# strip-namespace=yes drops metadata.namespace from every object, for a base whose
# kustomization sets the namespace and whose consumer overrides it. The kind overlay keeps
# the rendered namespaces, since one RoleBinding lives outside the operator namespace.
render() {
  local out="$1" values="$2" strip_ns="$3"
  shift 3
  local api_args=()
  local v
  for v in "$@"; do api_args+=(--api-versions "$v"); done
  mkdir -p "$out"
  helm template grid-operator "$CHART" \
    --namespace "$NAMESPACE_PLACEHOLDER" \
    --values "$values" \
    "${api_args[@]}" \
    --skip-tests \
    > "$out/all.yaml"
  # One file per object, named <kind>-<name>.yaml. Helm-only labels go; the
  # namespace goes too, since the kustomization sets it and a consumer overrides it.
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
      | del(.. | select(tag == \"!!map\" and has(\"app.kubernetes.io/managed-by\")) | .[\"app.kubernetes.io/managed-by\"])" \
      "$out/all.yaml" > "$file"
  done
  rm -f "$out/all.yaml"
}

# The platform base: OpenShift shape, image from params.env.
BASE="$TMP_DIR/operator"
render "$BASE" deploy/operator/values.yaml yes security.openshift.io/v1
IMAGE_DEFAULT="$(yq '.spec.template.spec.containers[0].image' "$BASE/deployment.yaml")"
cat > "$BASE/params.env" <<EOF
grid-operator-image=$IMAGE_DEFAULT
EOF
BASE_RESOURCES="$(resource_list "$BASE")"
{
  echo "apiVersion: kustomize.config.k8s.io/v1beta1"
  echo "kind: Kustomization"
  echo ""
  echo "# Generated from charts/grid-operator by scripts/generate-deployment-operator.sh."
  echo "# The namespace is a default; a platform operator or kubectl -n overrides it."
  echo "namespace: $NAMESPACE_PLACEHOLDER"
  echo ""
  echo "resources:"
  echo "$BASE_RESOURCES"
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
EOF
} > "$BASE/kustomization.yaml"

# The kind overlay: plain, with its own Namespace so kubectl apply -k works bare.
KIND="$TMP_DIR/kind"
render "$KIND" deploy/overlays/kind/values.yaml no
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
  cat <<'EOF'

labels:
  - pairs:
      app.kubernetes.io/name: grid-operator
      app.kubernetes.io/managed-by: kustomize
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
sync_dir "$KIND" "$REPO_ROOT/deploy/overlays/kind"

if $CHECK; then
  echo "deploy/operator and deploy/overlays/kind match the chart"
else
  echo "wrote deploy/operator and deploy/overlays/kind"
fi
