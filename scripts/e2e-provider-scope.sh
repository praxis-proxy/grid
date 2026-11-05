#!/usr/bin/env bash
# Sourced by e2e-hub-site.sh; uses its cluster, routing, and evidence helpers.

SCOPE_ALLOWLIST_FILE=""
SCOPE_RECOVERY_CREATED=0
SCOPE_PROBE_BIN=""

prepare_provider_scope_probe() {
  [[ $PROVIDER_SCOPE_TEST == 1 ]] || return 0
  cargo build --locked --manifest-path "$ROOT/Cargo.toml" -p swim --example provider_scope_probe \
    || die "build the test-only signed provider-scope probe"
  SCOPE_PROBE_BIN="$(cargo metadata --manifest-path "$ROOT/Cargo.toml" --no-deps --format-version 1 \
    | jq -er .target_directory)/debug/examples/provider_scope_probe"
}

# Only stable route identities are compared: expiry and metrics legitimately change.
scope_candidate_keys() {
  jq -cS --arg site "$SITE" --arg cluster "$2" \
    '[.candidates[]? | select(.site == $site and .cluster == $cluster)
      | {kind, name, site, cluster}] | sort_by(.kind, .name, .site, .cluster)' "$1"
}

scope_rejected_log() { grep -Eq 'reason="?provider_network_not_allowed("|[[:space:]]|$)' "$1"; }

scope_rejection_observed() {
  signed_operator_logs "$HUB_CTX" --since-time="$1" >"$2/rejection.log" || return 1
  scope_rejected_log "$2/rejection.log"
}

scope_routing_snapshot() {
  k "$HUB_CTX" get configmap grid-overlay-grid-grid-gateway -o json \
    | jq -er '.data["routing-config.json"]' >"$1"
}

scope_original_preserved() {
  scope_routing_snapshot "$1/current-routing.json" || return 1
  scope_candidate_keys "$1/current-routing.json" vcr >"$1/current-vcr.json" || return 1
  cmp -s "$1/before-vcr.json" "$1/current-vcr.json"
}

scope_recovery_present() {
  scope_routing_snapshot "$1/recovery-routing.json" || return 1
  scope_candidate_keys "$1/recovery-routing.json" scope-recovery \
    | jq -e --arg model "$MODEL" 'any(.[]; .kind == "inference_model" and .name == $model)' >/dev/null
}

scope_recovery_absent() {
  scope_routing_snapshot "$1/after-cleanup-routing.json" || return 1
  scope_candidate_keys "$1/after-cleanup-routing.json" scope-recovery | jq -e 'length == 0' >/dev/null
}

cleanup_provider_scope() {
  if [[ $SCOPE_RECOVERY_CREATED == 1 ]]; then
    k "$SITE_CTX" delete inferenceprovider scope-recovery --ignore-not-found --wait=true >/dev/null || return 1
    SCOPE_RECOVERY_CREATED=0
  fi
  if [[ -n $SCOPE_ALLOWLIST_FILE ]]; then
    k "$HUB_CTX" patch gridnetwork grid --type=json -p "$(jq -cn --slurpfile original "$SCOPE_ALLOWLIST_FILE" \
      '[{op: "replace", path: "/spec/consumerGateways/0/praxisConfig/deriveTopology/fromProviders", value: $original[0]}]')" \
      >/dev/null || return 1
    SCOPE_ALLOWLIST_FILE=""
  fi
}

assert_provider_scope() {
  [[ $PROVIDER_SCOPE_TEST == 1 ]] || return 0
  local dir="$ARTIFACTS/$MODE/provider-scope" private="$WORK/provider-scope-private" grid since patch
  mkdir -p "$dir" "$private"
  chmod 700 "$private"
  scope_routing_snapshot "$dir/before-routing.json" || die "capture the original signed provider routes"
  scope_candidate_keys "$dir/before-routing.json" vcr >"$dir/before-vcr.json"
  jq -e 'length > 0' "$dir/before-vcr.json" >/dev/null || die "scope test needs a legitimate remote vcr candidate"
  k "$HUB_CTX" get gridnetwork grid -o json >"$dir/before-network.json"
  grid=$(k "$SITE_CTX" get gridnetwork grid -o jsonpath='{.spec.gridId}')
  [[ -n $grid && $grid == "$(jq -r '.spec.gridId' "$dir/before-network.json")" ]] \
    || die "scope probe must use the existing authenticated grid identity"
  (
    umask 077
    k "$SITE_CTX" get secret grid-site-identity -o jsonpath='{.data.tls\.key}' | base64 -d >"$private/site.key"
    k "$SITE_CTX" get secret grid-swim-key -o jsonpath='{.data.key}' | base64 -d >"$private/mesh.key"
  ) || die "load the enrolled site and mesh keys into private temporary files"
  jq -n --arg origin "$SITE" --arg grid "$grid" --arg target "$HUB_SWIM_IP:7946" --arg model "$MODEL" \
    '{origin_site: $origin, grid_id: $grid, network_id: "scope-probe-denied", provider_id: "vcr", model: $model, target: $target}' \
    >"$dir/probe-config.json"
  since=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # Word splitting is the command-prefix convention used by lb_curl as well.
  # shellcheck disable=SC2086
  ${NET_PREFIX:-} "$SCOPE_PROBE_BIN" "$dir/probe-config.json" "$private/site.key" "$private/mesh.key" \
    >"$dir/probe.json" 2>"$dir/probe.stderr" || die "deliver the encrypted, correctly signed adversarial update"
  jq -e '.broadcast_datagrams > 0' "$dir/probe.json" >/dev/null || die "probe sent no custom broadcast"
  eventually "hub verifies the site signature and rejects the provider network scope" \
    scope_rejection_observed "$since" "$dir" || return
  scope_original_preserved "$dir" || { fail "scope rejection must preserve the original remote candidate set"; return; }
  pass "scope rejection preserves the original remote candidates"
  eventually "original remote inference still succeeds after scope rejection" consumer_served || return
  cp "$WORK/consumer.hdr" "$WORK/consumer.body" "$dir/"

  SCOPE_ALLOWLIST_FILE="$WORK/scope-original-allowlist.json"
  jq -e '.spec.consumerGateways[0].praxisConfig.deriveTopology.fromProviders' "$dir/before-network.json" \
    >"$SCOPE_ALLOWLIST_FILE" || die "save the hub provider allowlist"
  patch=$(jq -cn --slurpfile original "$SCOPE_ALLOWLIST_FILE" \
    '[{op: "replace", path: "/spec/consumerGateways/0/praxisConfig/deriveTopology/fromProviders",
       value: ($original[0] + ["scope-recovery"])}]')
  k "$HUB_CTX" patch gridnetwork grid --type=json -p "$patch" >/dev/null || die "allow the named recovery provider"
  k "$SITE_CTX" get inferenceprovider vcr -o json \
    | jq --arg model "$MODEL" '
        .metadata = {name: "scope-recovery", labels: {"grid.praxis.fast/e2e-scope-probe": "true"}}
        | del(.status, .spec.modelDiscovery, .spec.routingClusterRef)
        | .spec.clusterName = "scope-recovery" | .spec.models = [{name: $model}]' >"$dir/recovery-provider.json"
  SCOPE_RECOVERY_CREATED=1
  k "$SITE_CTX" create -f "$dir/recovery-provider.json" >/dev/null || die "publish a fresh legitimate provider after the attack"
  eventually "hub accepts the named legitimate provider after rejecting a higher revision" \
    scope_recovery_present "$dir" || {
      cleanup_provider_scope || fail "clean up after recovery provider timeout"
      return 1
    }
  scope_original_preserved "$dir" || {
    fail "fresh valid state must retain the original provider"
    cleanup_provider_scope || fail "clean up after provider preservation failure"
    return 1
  }
  k "$SITE_CTX" delete inferenceprovider scope-recovery --wait=true >/dev/null || die "withdraw the recovery provider"
  SCOPE_RECOVERY_CREATED=0
  eventually "hub accepts the recovery provider withdrawal" scope_recovery_absent "$dir" || {
    cleanup_provider_scope || fail "clean up after recovery withdrawal timeout"
    return 1
  }
  cleanup_provider_scope || { fail "restore the provider-scope qualification allowlist"; return; }
  eventually "original remote inference survives valid updates and withdrawal" consumer_served || return
  cp "$WORK/consumer.hdr" "$dir/after-cleanup.hdr"
  cp "$WORK/consumer.body" "$dir/after-cleanup.body"
  k "$HUB_CTX" get gridnetwork grid -o json >"$dir/after-network.json"
}

# Exercise the evidence predicates without a cluster, including an unsigned-refusal false positive.
provider_scope_selftest() {
  local dir="$WORK/provider-scope-selftest"
  mkdir -p "$dir"
  printf '%s\n' '{"candidates":[{"kind":"inference_model","name":"model","site":"site-a","cluster":"vcr"}]}' \
    >"$dir/routes.json"
  want "scope candidate identity is nonempty" 1 "$(scope_candidate_keys "$dir/routes.json" vcr | jq length)"
  want "absent recovery provider cannot count as accepted" 0 "$(scope_candidate_keys "$dir/routes.json" scope-recovery | jq length)"
  scope_candidate_keys "$dir/routes.json" vcr >"$dir/before.json"
  printf '%s\n' '{"candidates":[]}' >"$dir/empty.json"
  scope_candidate_keys "$dir/empty.json" vcr >"$dir/after.json"
  if cmp -s "$dir/before.json" "$dir/after.json"; then
    fail "a withdrawn original provider must fail the preservation check"
  else
    pass "original provider withdrawal fails the preservation check"
  fi
  jq '.candidates[0].cluster = "scope-recovery"' "$dir/routes.json" >"$dir/recovery.json"
  want "a distinct named recovery provider is observable" 1 "$(scope_candidate_keys "$dir/recovery.json" scope-recovery | jq length)"
  printf '%s\n' 'rejecting pinned-origin state broadcast reason="missing_signature"' >"$dir/rejection.log"
  if scope_rejected_log "$dir/rejection.log"; then
    fail "unsigned refusal must not prove signed provider-scope enforcement"
  else
    pass "unsigned refusal does not prove scope enforcement"
  fi
  printf '%s\n' 'rejecting pinned-origin state broadcast reason="provider_network_not_allowed"' >"$dir/rejection.log"
  if scope_rejected_log "$dir/rejection.log"; then
    pass "scope refusal has the specific authorization reason"
  else
    fail "scope authorization reason was not recognized"
  fi
  printf '%s\n' 'reason=provider_network_not_allowed' >"$dir/rejection.log"
  if scope_rejected_log "$dir/rejection.log"; then
    pass "unquoted structured scope reasons are recognized"
  else
    fail "unquoted scope authorization reason was not recognized"
  fi
}
