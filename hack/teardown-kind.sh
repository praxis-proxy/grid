#!/usr/bin/env bash
set -euo pipefail

CLUSTER_NAME="${KIND_CLUSTER_NAME:-praxis-dev}"

echo "==> Deleting KIND cluster '${CLUSTER_NAME}'..."
kind delete cluster --name "${CLUSTER_NAME}"
echo "==> Done."
