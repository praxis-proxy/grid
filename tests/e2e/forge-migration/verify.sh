#!/usr/bin/env bash
# Forge resolves this script from the config root and preserves the caller cwd.
set -euo pipefail
CONFIG_ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
[[ -f $CONFIG_ROOT/forge.yaml && -f $CONFIG_ROOT/proof.yaml ]]
printf '%s\n' "$PWD" > "$CONFIG_ROOT/execution-cwd.txt"
printf '%s\n' "$CONFIG_ROOT" > "$CONFIG_ROOT/execution-root.txt"
