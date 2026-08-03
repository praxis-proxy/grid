#!/usr/bin/env bash
# Validate stock IPP path: 401 without key; API key + internal chat completion 200.
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_cmd kubectl
require_cmd jq
require_cmd curl

PASS=0
FAIL=0
TOTAL=0

check() {
  TOTAL=$((TOTAL + 1))
  local name="$1"
  shift
  if eval "$@"; then
    ok "$name"
    PASS=$((PASS + 1))
  else
    fail "$name"
    FAIL=$((FAIL + 1))
  fi
}

echo -e "${BOLD}MaaS IPP lab validation${NC}"
echo "  context: $KUBE_CONTEXT"

check "maas-api ready" \
  'kc get deployment maas-api -n "$MAAS_NAMESPACE" -o jsonpath="{.status.readyReplicas}" | grep -q 1'
check "maas-controller ready" \
  'kc get deployment maas-controller -n "$MAAS_NAMESPACE" -o jsonpath="{.status.readyReplicas}" | grep -q 1'
check "payload-processing (IPP) ready" \
  'kc get deployment payload-processing -n "$GATEWAY_NAMESPACE" -o jsonpath="{.status.readyReplicas}" | grep -q 1'
check "Authorino ready" \
  'kc get deployment authorino -n kuadrant-system -o jsonpath="{.status.readyReplicas}" | grep -q 1'
check "Gateway programmed" \
  'kc get gateway maas-default-gateway -n "$GATEWAY_NAMESPACE" -o jsonpath="{.status.conditions[?(@.type==\"Programmed\")].status}" | grep -q True'

pkill -f "port-forward.*19090" 2>/dev/null || true
sleep 1
kc port-forward -n "$GATEWAY_NAMESPACE" svc/maas-default-gateway-istio 19090:80 >/dev/null 2>&1 &
PF_PID=$!
trap 'kill "$PF_PID" 2>/dev/null || true' EXIT
sleep 3

API_KEY=$(kc exec -n "$MAAS_NAMESPACE" deployment/maas-api -- curl -sk \
  "https://localhost:8443/v1/api-keys" \
  -H "X-MaaS-Username: validate-user" \
  -H 'X-MaaS-Group: ["system:authenticated"]' \
  -H "Content-Type: application/json" \
  -d '{"name":"forge-validate"}' 2>/dev/null | jq -r '.key // empty')

check "API key creation" '[[ -n "$API_KEY" ]]'

TOTAL=$((TOTAL + 1))
HTTP_NO_KEY=$(curl -s -o /dev/null -w '%{http_code}' --max-time 5 http://localhost:19090/v1/models || echo "000")
if [[ "$HTTP_NO_KEY" == "401" ]]; then
  ok "No API key → 401"
  PASS=$((PASS + 1))
else
  fail "No API key → HTTP $HTTP_NO_KEY (expected 401)"
  FAIL=$((FAIL + 1))
fi

TOTAL=$((TOTAL + 1))
INT_RESP=$(curl -s --max-time 30 http://localhost:19090/llm-internal/sim-internal/v1/chat/completions \
  -H "Authorization: Bearer $API_KEY" -H "Content-Type: application/json" \
  -d '{"model":"facebook/opt-125m","messages":[{"role":"user","content":"validate"}],"max_tokens":5}' 2>/dev/null || true)
if echo "$INT_RESP" | jq -e '.choices[0].message.content' &>/dev/null; then
  ok "Internal model inference (llm-d sim)"
  PASS=$((PASS + 1))
else
  fail "Internal model inference: ${INT_RESP:-no response}"
  FAIL=$((FAIL + 1))
fi

echo ""
echo -e "${BOLD}Result: ${PASS}/${TOTAL} passed, ${FAIL} failed${NC}"
[[ "$FAIL" -eq 0 ]]
