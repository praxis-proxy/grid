#!/usr/bin/env bash
# Verify a Grid installation on existing clusters.
# Checks pod health, SWIM membership, overlay convergence, and routing.
#
# Usage: ./verify.sh <inventory.yaml>
#
# Requires: kubectl, yq, jq

set -euo pipefail

INVENTORY="${1:?Usage: $0 <inventory.yaml>}"

if [[ ! -f "$INVENTORY" ]]; then
  echo "ERROR: inventory file not found: $INVENTORY" >&2
  exit 1
fi

TOPOLOGY=$(yq eval '.topology' "$INVENTORY")
SITE_NAMES=$(yq eval '.sites | keys | .[]' "$INVENTORY")
SITE_COUNT=$(echo "$SITE_NAMES" | wc -w)
ERRORS=0

check() {
  local desc="$1"
  shift
  if "$@" &>/dev/null; then
    echo "  PASS  $desc"
  else
    echo "  FAIL  $desc" >&2
    ERRORS=$((ERRORS + 1))
  fi
}

echo "Verifying Grid installation"
echo "  Topology: $TOPOLOGY"
echo "  Sites:    $SITE_COUNT"
echo ""

for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")
  echo "--- Site: $SITE ---"

  check "namespace exists" kubectl --context "$CONTEXT" get namespace grid-system

  pod_phase() {
    kubectl --context "$1" -n grid-system \
      get pods -l "$2" -o jsonpath='{.items[0].status.phase}' 2>/dev/null
  }

  check "operator pod running" test "$(pod_phase "$CONTEXT" app.kubernetes.io/name=grid-operator)" = "Running"

  if [[ "$TOPOLOGY" == "dedicated-edge" ]]; then
    ROLE=$(yq eval ".sites.${SITE}.roles[0]" "$INVENTORY" 2>/dev/null || echo "unknown")
    if [[ "$ROLE" == "consumer" ]]; then
      check "consumer gateway running" test "$(pod_phase "$CONTEXT" app.kubernetes.io/instance=consumer-gateway)" = "Running"
    elif [[ "$ROLE" == "provider" ]]; then
      check "provider gateway running" test "$(pod_phase "$CONTEXT" app.kubernetes.io/instance=provider-gateway)" = "Running"
    fi
  elif [[ "$TOPOLOGY" == "combined-site" ]]; then
    check "consumer gateway running" test "$(pod_phase "$CONTEXT" app.kubernetes.io/instance=consumer-gateway)" = "Running"
    check "provider gateway running" test "$(pod_phase "$CONTEXT" app.kubernetes.io/instance=provider-gateway)" = "Running"
  fi

  check "SWIM service exists" kubectl --context "$CONTEXT" -n grid-system \
    get service grid-operator-swim

  SWIM_LB=$(kubectl --context "$CONTEXT" -n grid-system \
    get service grid-operator-swim -o jsonpath='{.status.loadBalancer.ingress[0].ip}' 2>/dev/null || echo "")
  if [[ -n "$SWIM_LB" ]]; then
    echo "  PASS  SWIM LoadBalancer IP assigned: $SWIM_LB"
  else
    echo "  WARN  SWIM LoadBalancer IP not yet assigned (may be pending)" >&2
  fi

  echo ""
done

echo "--- Overlay convergence ---"
for SITE in $SITE_NAMES; do
  CONTEXT=$(yq eval ".sites.${SITE}.context" "$INVENTORY")

  OVERLAY_CM=$(kubectl --context "$CONTEXT" -n grid-system \
    get configmap -l grid.praxis-proxy.io/network \
    -o jsonpath='{.items[0].metadata.name}' 2>/dev/null || echo "")
  if [[ -n "$OVERLAY_CM" ]]; then
    echo "  PASS  $SITE: routing overlay present ($OVERLAY_CM)"

    OVERLAY_JSON=$(kubectl --context "$CONTEXT" -n grid-system \
      get configmap "$OVERLAY_CM" \
      -o jsonpath='{.data.routing-config\.json}' 2>/dev/null || echo "")
    if [[ -n "$OVERLAY_JSON" ]]; then
      CANDIDATE_INFO=$(echo "$OVERLAY_JSON" \
        | jq -r '.candidates[] | .name + " stable_id=" + .stable_id' 2>/dev/null || echo "")
      if [[ -n "$CANDIDATE_INFO" ]]; then
        echo "  INFO  $SITE: overlay candidates (use stable_id as provider candidate_id):"
        echo "$CANDIDATE_INFO" | while IFS= read -r line; do echo "        $line"; done
      fi
    fi
  else
    echo "  FAIL  $SITE: routing overlay not found" >&2
    ERRORS=$((ERRORS + 1))
  fi

  # Cross-check overlay clusters against consumer config.
  if [[ -n "$OVERLAY_JSON" && "$TOPOLOGY" == "combined-site" ]]; then
    OVERLAY_CLUSTERS=$(echo "$OVERLAY_JSON" \
      | jq -r '[.candidates[].cluster] | unique | .[]' 2>/dev/null || echo "")
    CONSUMER_CONFIG=$(kubectl --context "$CONTEXT" -n grid-system \
      get configmap consumer-praxis-config \
      -o jsonpath='{.data.praxis\.yaml}' 2>/dev/null || echo "")
    if [[ -n "$CONSUMER_CONFIG" && -n "$OVERLAY_CLUSTERS" ]]; then
      HOP_CLUSTERS=$(echo "$CONSUMER_CONFIG" \
        | yq eval '.filter_chains[].filters[] | select(.filter == "intelligent_route") | .provider_hop_clusters[]' 2>/dev/null || echo "")
      while IFS= read -r OC; do
        [[ -z "$OC" ]] && continue
        if ! echo "$HOP_CLUSTERS" | grep -qw "$OC"; then
          echo "  WARN  $SITE: overlay cluster '$OC' not in consumer provider_hop_clusters — requests to this cluster will fail" >&2
        fi
      done <<< "$OVERLAY_CLUSTERS"
    fi
  fi
done
echo ""

echo "--- Request test ---"
CONSUMER_SITE=""
for SITE in $SITE_NAMES; do
  if [[ "$TOPOLOGY" == "combined-site" ]]; then
    CONSUMER_SITE="$SITE"
    break
  fi
  ROLE=$(yq eval ".sites.${SITE}.roles[0]" "$INVENTORY" 2>/dev/null || echo "")
  if [[ "$ROLE" == "consumer" ]]; then
    CONSUMER_SITE="$SITE"
    break
  fi
done

if [[ -n "$CONSUMER_SITE" ]]; then
  CONTEXT=$(yq eval ".sites.${CONSUMER_SITE}.context" "$INVENTORY")

  TEST_MODEL=$(kubectl --context "$CONTEXT" -n grid-system \
    get configmap -l grid.praxis-proxy.io/network \
    -o jsonpath='{.items[0].data.routing-config\.json}' 2>/dev/null \
    | jq -r '.candidates[0].name // empty' 2>/dev/null || echo "")
  if [[ -z "$TEST_MODEL" ]]; then
    TEST_MODEL="sim-model-v1"
  fi
  echo "  Sending test request from $CONSUMER_SITE (model: $TEST_MODEL)..."

  JOB_NAME="grid-verify-$(date +%s)"
  kubectl --context "$CONTEXT" -n grid-system create -f - <<EOF
apiVersion: batch/v1
kind: Job
metadata:
  name: $JOB_NAME
  namespace: grid-system
spec:
  backoffLimit: 0
  ttlSecondsAfterFinished: 60
  template:
    spec:
      restartPolicy: Never
      containers:
      - name: curl
        image: curlimages/curl:8.12.1
        command:
        - curl
        - -sf
        - -X
        - POST
        - -H
        - "Content-Type: application/json"
        - -d
        - '{"model":"$TEST_MODEL","messages":[{"role":"user","content":"verify"}]}'
        - http://consumer-gateway.grid-system.svc.cluster.local:8080/v1/chat/completions
        securityContext:
          runAsNonRoot: true
          readOnlyRootFilesystem: true
          allowPrivilegeEscalation: false
          capabilities:
            drop: [ALL]
EOF

  if kubectl --context "$CONTEXT" -n grid-system wait --for=condition=complete \
    "job/$JOB_NAME" --timeout=30s &>/dev/null; then
    echo "  PASS  test request succeeded"
    RESPONSE=$(kubectl --context "$CONTEXT" -n grid-system logs "job/$JOB_NAME" 2>/dev/null || echo "")
    if echo "$RESPONSE" | grep -q "choices"; then
      echo "  PASS  response contains expected inference fields"
    else
      echo "  FAIL  response missing 'choices' field — routing succeeded but did not reach inference" >&2
      ERRORS=$((ERRORS + 1))
    fi
  else
    echo "  FAIL  test request did not complete within 30s" >&2
    ERRORS=$((ERRORS + 1))
  fi

  kubectl --context "$CONTEXT" -n grid-system delete "job/$JOB_NAME" --ignore-not-found &>/dev/null
else
  echo "  SKIP  no consumer site found for request test" >&2
fi
echo ""

if [[ "$ERRORS" -gt 0 ]]; then
  echo "Verification FAILED with $ERRORS error(s)." >&2
  exit 1
fi

echo "Verification PASSED. All $SITE_COUNT sites healthy."
