#!/usr/bin/env bash
# Deploy stock MaaS Kind fixtures: external model, llm-d sim LLMIS, subscription/auth.
set -euo pipefail
# shellcheck source=lib.sh
source "$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)/lib.sh"
parse_context "$@"
require_cmd kubectl

LLM_KATAN_FQDN="${LLM_KATAN_FQDN:-3-147-232-199.sslip.io}"
MODEL_NAMESPACE="llm"
INTERNAL_MODEL_NAMESPACE="llm-internal"

kc create namespace "$MODEL_NAMESPACE" --dry-run=client -o yaml | kc apply -f -
kc create namespace "$INTERNAL_MODEL_NAMESPACE" --dry-run=client -o yaml | kc apply -f -
kc create namespace "$SUBSCRIPTION_NAMESPACE" --dry-run=client -o yaml | kc apply -f -

if ! kc get externalmodel llm-katan-openai -n "$MODEL_NAMESPACE" &>/dev/null; then
  kc apply -f - <<EOF
apiVersion: v1
kind: Secret
metadata:
  name: llm-katan-creds
  namespace: ${MODEL_NAMESPACE}
  labels:
    inference.llm-d.ai/ipp-managed: "true"
stringData:
  api-key: "llm-katan-openai-key"
---
apiVersion: maas.opendatahub.io/v1alpha1
kind: ExternalModel
metadata:
  name: llm-katan-openai
  namespace: ${MODEL_NAMESPACE}
spec:
  endpoint: "${LLM_KATAN_FQDN}"
  provider: openai
  targetModel: llm-katan-echo
  credentialRef:
    name: llm-katan-creds
---
apiVersion: maas.opendatahub.io/v1alpha1
kind: MaaSModelRef
metadata:
  name: llm-katan-openai
  namespace: ${MODEL_NAMESPACE}
spec:
  modelRef:
    kind: ExternalModel
    name: llm-katan-openai
EOF
  ok "External model fixtures applied"
else
  ok "External model fixtures already present"
fi

if ! kc get llminferenceservice sim-internal -n "$INTERNAL_MODEL_NAMESPACE" &>/dev/null; then
  wait_for_llmisvc_webhook "$INTERNAL_MODEL_NAMESPACE"
  _retries=0
  while [[ $_retries -lt 12 ]]; do
    if kc apply -f - <<EOF
apiVersion: serving.kserve.io/v1alpha1
kind: LLMInferenceService
metadata:
  name: sim-internal
  namespace: ${INTERNAL_MODEL_NAMESPACE}
spec:
  model:
    uri: hf://placeholder/no-model
    name: facebook/opt-125m
  storageInitializer:
    enabled: false
  replicas: 1
  router:
    route: {}
    gateway:
      refs:
        - name: maas-default-gateway
          namespace: ${GATEWAY_NAMESPACE}
  template:
    containers:
      - name: main
        image: "ghcr.io/llm-d/llm-d-inference-sim:v0.7.1"
        command: ["/app/llm-d-inference-sim"]
        args:
        - --port
        - "8000"
        - --model
        - facebook/opt-125m
        - --mode
        - random
        env:
          - name: POD_NAME
            valueFrom:
              fieldRef:
                apiVersion: v1
                fieldPath: metadata.name
          - name: POD_NAMESPACE
            valueFrom:
              fieldRef:
                apiVersion: v1
                fieldPath: metadata.namespace
        ports:
          - name: http
            containerPort: 8000
            protocol: TCP
        resources:
          requests:
            cpu: 100m
            memory: 256Mi
          limits:
            cpu: 500m
            memory: 512Mi
---
apiVersion: maas.opendatahub.io/v1alpha1
kind: MaaSModelRef
metadata:
  name: sim-internal
  namespace: ${INTERNAL_MODEL_NAMESPACE}
spec:
  modelRef:
    kind: LLMInferenceService
    name: sim-internal
EOF
    then
      ok "Internal llm-d sim fixtures applied"
      break
    fi
    warn "LLMIS apply failed (webhook race?); retrying..."
    sleep 5
    _retries=$((_retries + 1))
  done
  [[ $_retries -lt 12 ]] || die "failed to apply internal llm-d sim fixtures"
else
  ok "Internal model fixtures already present"
fi

if ! kc get maassubscription simulator-subscription -n "$SUBSCRIPTION_NAMESPACE" &>/dev/null; then
  kc apply -f - <<EOF
apiVersion: maas.opendatahub.io/v1alpha1
kind: MaaSSubscription
metadata:
  name: simulator-subscription
  namespace: ${SUBSCRIPTION_NAMESPACE}
spec:
  owner:
    groups:
      - name: system:authenticated
    users: []
  modelRefs:
    - name: llm-katan-openai
      namespace: ${MODEL_NAMESPACE}
      tokenRateLimits:
        - limit: 100
          window: 1m
    - name: sim-internal
      namespace: ${INTERNAL_MODEL_NAMESPACE}
      tokenRateLimits:
        - limit: 100
          window: 1m
  priority: 10
---
apiVersion: maas.opendatahub.io/v1alpha1
kind: MaaSAuthPolicy
metadata:
  name: simulator-access
  namespace: ${SUBSCRIPTION_NAMESPACE}
spec:
  modelRefs:
    - name: llm-katan-openai
      namespace: ${MODEL_NAMESPACE}
    - name: sim-internal
      namespace: ${INTERNAL_MODEL_NAMESPACE}
  subjects:
    groups:
      - name: system:authenticated
    users: []
EOF
  ok "Subscription + MaaSAuthPolicy applied"
else
  ok "Subscription fixtures already present"
fi

echo "  Waiting for controller reconciliation..."
sleep 20
EXTERNAL_PHASE=$(kc get maasmodelref llm-katan-openai -n "$MODEL_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
INTERNAL_PHASE=$(kc get maasmodelref sim-internal -n "$INTERNAL_MODEL_NAMESPACE" -o jsonpath='{.status.phase}' 2>/dev/null || true)
[[ "$EXTERNAL_PHASE" == "Ready" ]] && ok "External model: Ready" || warn "External model: ${EXTERNAL_PHASE:-unknown}"
[[ "$INTERNAL_PHASE" == "Ready" ]] && ok "Internal model: Ready" || warn "Internal model: ${INTERNAL_PHASE:-Pending}"
