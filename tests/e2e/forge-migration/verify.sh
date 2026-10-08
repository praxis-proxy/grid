#!/usr/bin/env bash
# Forge must execute this relative to the relocated configuration directory.
set -euo pipefail
[[ -f forge.yaml && -f proof.yaml ]]
printf '%s\n' "$PWD" > execution-cwd.txt
