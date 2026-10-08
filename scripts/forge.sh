#!/usr/bin/env bash
# Install or run Grid's pinned upstream Forge. Only an explicit FORGE_BIN may
# select another build; an unrelated PATH or old target/debug binary is ignored.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
REPOSITORY=https://github.com/praxis-proxy/forge

fail() { printf '%s\n' "$*" >&2; exit 1; }

resolve_override() {
  local candidate=$FORGE_BIN
  if [[ $candidate != */* ]]; then
    candidate=$(command -v -- "$candidate") || fail "FORGE_BIN is not on PATH: $FORGE_BIN"
  fi
  [[ -f $candidate && -x $candidate ]] || fail "FORGE_BIN is not an executable file: $FORGE_BIN"
  candidate=$(cd "$(dirname "$candidate")" && pwd)/$(basename "$candidate")
  printf 'Forge: %s (explicit FORGE_BIN override; upstream pin not enforced)\n' "$candidate" >&2
  printf '%s\n' "$candidate"
}

cache_context() {
  REVISION=$(cat "$ROOT/scripts/forge-revision")
  [[ $REVISION =~ ^[0-9a-f]{40}$ ]] || fail \
    "scripts/forge-revision must contain one full upstream commit SHA."
  local compiler digest host
  compiler=$(cd "$ROOT" && rustc -vV)
  digest=$(printf '%s\n' "$compiler" | sha256sum)
  TOOLCHAIN=${digest%% *}
  host=$(sed -n 's/^host: //p' <<<"$compiler")
  [[ -n $host ]] || fail "Cannot determine the Rust host target"
  HOST_TARGET=$host
  INSTALL_DIR=$ROOT/target/tools/forge/$REVISION/$TOOLCHAIN
  BINARY=$INSTALL_DIR/bin/praxis-forge
}

source_identity() {
  printf '%s\n' "$REPOSITORY" "$REVISION" "$HOST_TARGET" "$TOOLCHAIN"
}

cache_valid() {
  [[ -x $BINARY && -f $INSTALL_DIR/source && -f $INSTALL_DIR/binary.sha256 ]] || return 1
  cmp -s "$INSTALL_DIR/source" <(source_identity) || return 1
  (cd "$INSTALL_DIR" && sha256sum --check --status binary.sha256)
}

install_forge() {
  if ! cache_valid; then
    mkdir -p "$INSTALL_DIR"
    cargo install --locked --force --git "$REPOSITORY" --rev "$REVISION" \
      --bin praxis-forge --target "$HOST_TARGET" --root "$INSTALL_DIR" \
      --target-dir "$ROOT/target/forge-build" forge >&2
    source_identity > "$INSTALL_DIR/source"
    (cd "$INSTALL_DIR" && sha256sum bin/praxis-forge > binary.sha256)
  fi
  cache_valid || fail "Forge installation did not produce a verified executable"
}

if [[ ${1:-} == install ]]; then
  [[ $# == 1 ]] || fail 'Usage: scripts/forge.sh install'
  [[ -z ${FORGE_BIN:-} ]] || fail 'Unset FORGE_BIN before installing the pinned upstream Forge'
  cache_context
  (cd "$ROOT" && install_forge)
  printf 'Installed Forge: %s (upstream %s)\n' "$BINARY" "$REVISION" >&2
  exit 0
fi

if [[ -n ${FORGE_BIN:-} ]]; then
  BINARY=$(resolve_override)
else
  cache_context
  cache_valid || fail "Pinned Forge is missing or invalid; run: $ROOT/scripts/forge.sh install"
  printf 'Forge: %s (upstream %s, compiler %s)\n' "$BINARY" "$REVISION" "$TOOLCHAIN" >&2
fi

if [[ ${1:-} == path ]]; then
  [[ $# == 1 ]] || fail 'Usage: scripts/forge.sh path'
  printf '%s\n' "$BINARY"
else
  exec "$BINARY" "$@"
fi
