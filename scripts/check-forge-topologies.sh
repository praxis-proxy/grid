#!/usr/bin/env bash
# Compare every real topology across the retired Grid CLI and pinned upstream.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
FORGE_BIN=$("$ROOT/scripts/forge.sh" path)
: "${FORGE_BASELINE_BIN:?Set FORGE_BASELINE_BIN to Grid a4a7d7168ec36e59f31bcb9be0d11d6dd69c4974 praxis-forge}"
[[ $FORGE_BASELINE_BIN == /* && -x $FORGE_BASELINE_BIN ]]
EVIDENCE=$ROOT/target/forge-compatibility
WORK=$(mktemp -d)
trap 'rm -rf "$WORK"' EXIT
mkdir -p "$EVIDENCE"

for config in "$ROOT"/tests/e2e/topologies/*/forge.yaml; do
  topology=$(basename "$(dirname "$config")")
  printf 'Checking Forge topology: %s\n' "$topology"
  for implementation in grid upstream; do
    binary=$FORGE_BIN
    [[ $implementation != grid ]] || binary=$FORGE_BASELINE_BIN
    for location in local absolute; do
      (
        if [[ $location == local ]]; then
          cd "$(dirname "$config")"
          selected=forge.yaml
        else
          cd "$WORK"
          selected=$config
        fi
        "$binary" --config "$selected" config validate > "$EVIDENCE/$topology.$implementation.$location.validate.txt"
        "$binary" --config "$selected" --output json plan | jq --sort-keys . > "$EVIDENCE/$topology.$implementation.$location.plan.json"
        "$binary" --config "$selected" --output json config show | jq --sort-keys . > "$EVIDENCE/$topology.$implementation.$location.config.json"
      )
      for output in plan config; do
        cmp "$EVIDENCE/$topology.grid.local.$output.json" "$EVIDENCE/$topology.$implementation.$location.$output.json"
      done
    done
  done
done
