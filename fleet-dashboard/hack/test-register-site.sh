#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Tests for hack/lib/registry.sh merge and remove logic. Runs without a cluster.
set -euo pipefail

ROOT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
# shellcheck source=lib/registry.sh
source "$ROOT/hack/lib/registry.sh"
require_yq

tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail=0

# normalize: JSON with sorted map keys, so key order inside maps does not matter
# while array order (entry position) still does.
normalize() { yq -o=json 'sort_keys(..)' "$1"; }

assert_same() { # assert_same <case> <expected-file> <actual-file>
  if diff -u <(normalize "$2") <(normalize "$3") >"$tmp/diff.out"; then
    echo "ok   $1"
  else
    echo "FAIL $1"; cat "$tmp/diff.out"; fail=1
  fi
}

cat >"$tmp/entry-a.yaml" <<'EOF'
name: site-a
namespace: clusters
address: gateway.apps.a.example.com
port: "443"
labels:
  region: us-east-2
  metricsURL: https://thanos-querier-openshift-monitoring.apps.a.example.com
  metricsSecret: site-site-a
EOF

cat >"$tmp/entry-b.yaml" <<'EOF'
name: site-b
namespace: clusters
address: gateway.apps.b.example.com
port: "443"
labels:
  region: eu-west-2
  displayName: London
  metricsURL: https://thanos-querier-openshift-monitoring.apps.b.example.com
  metricsSecret: site-site-b
EOF

# Case 1: empty file gets endpoints: [A]
: >"$tmp/case1.yaml"
merge_site_entry "$tmp/case1.yaml" "$tmp/entry-a.yaml"
cat >"$tmp/case1.expected.yaml" <<'EOF'
endpoints:
  - name: site-a
    namespace: clusters
    address: gateway.apps.a.example.com
    port: "443"
    labels:
      region: us-east-2
      metricsURL: https://thanos-querier-openshift-monitoring.apps.a.example.com
      metricsSecret: site-site-a
EOF
assert_same "empty file" "$tmp/case1.expected.yaml" "$tmp/case1.yaml"

# Case 2: new entry is appended after existing ones
cp "$tmp/case1.expected.yaml" "$tmp/case2.yaml"
merge_site_entry "$tmp/case2.yaml" "$tmp/entry-b.yaml"
cat >"$tmp/case2.expected.yaml" <<'EOF'
endpoints:
  - name: site-a
    namespace: clusters
    address: gateway.apps.a.example.com
    port: "443"
    labels:
      region: us-east-2
      metricsURL: https://thanos-querier-openshift-monitoring.apps.a.example.com
      metricsSecret: site-site-a
  - name: site-b
    namespace: clusters
    address: gateway.apps.b.example.com
    port: "443"
    labels:
      region: eu-west-2
      displayName: London
      metricsURL: https://thanos-querier-openshift-monitoring.apps.b.example.com
      metricsSecret: site-site-b
EOF
assert_same "new entry appended" "$tmp/case2.expected.yaml" "$tmp/case2.yaml"

# Case 3: existing entry is replaced in place; unmanaged labels survive; managed ones are overwritten
cat >"$tmp/case3.yaml" <<'EOF'
endpoints:
  - name: site-a
    namespace: clusters
    address: old-gateway.apps.a.example.com
    port: "8443"
    labels:
      region: eu-west-1
      metricsAddress: 10.0.0.1
      metricsPort: "9090"
      metricsSecret: site-old
  - name: site-b
    namespace: clusters
    address: gateway.apps.b.example.com
    port: "443"
    labels:
      region: eu-west-2
EOF
merge_site_entry "$tmp/case3.yaml" "$tmp/entry-a.yaml"
cat >"$tmp/case3.expected.yaml" <<'EOF'
endpoints:
  - name: site-a
    namespace: clusters
    address: gateway.apps.a.example.com
    port: "443"
    labels:
      region: us-east-2
      metricsAddress: 10.0.0.1
      metricsPort: "9090"
      metricsURL: https://thanos-querier-openshift-monitoring.apps.a.example.com
      metricsSecret: site-site-a
  - name: site-b
    namespace: clusters
    address: gateway.apps.b.example.com
    port: "443"
    labels:
      region: eu-west-2
EOF
assert_same "replace existing in place" "$tmp/case3.expected.yaml" "$tmp/case3.yaml"

# Case 4: merging the same entry again changes nothing
cp "$tmp/case3.yaml" "$tmp/case4.before.yaml"
merge_site_entry "$tmp/case3.yaml" "$tmp/entry-a.yaml"
assert_same "idempotent re-merge" "$tmp/case4.before.yaml" "$tmp/case3.yaml"
# Also verify byte-level identity (not just structural equivalence)
if cmp -s "$tmp/case4.before.yaml" "$tmp/case3.yaml"; then
  echo "ok   byte-level idempotence"
else
  echo "FAIL byte-level idempotence"
  diff -u "$tmp/case4.before.yaml" "$tmp/case3.yaml"
  fail=1
fi

# Case 5: remove drops only the named entry; removing a missing name is a no-op
remove_site_entry "$tmp/case3.yaml" site-a
remove_site_entry "$tmp/case3.yaml" site-does-not-exist
cat >"$tmp/case5.expected.yaml" <<'EOF'
endpoints:
  - name: site-b
    namespace: clusters
    address: gateway.apps.b.example.com
    port: "443"
    labels:
      region: eu-west-2
EOF
assert_same "remove entry" "$tmp/case5.expected.yaml" "$tmp/case3.yaml"

# Case 6: remove last entry leaves empty endpoints list
cat >"$tmp/case6.yaml" <<'EOF'
endpoints:
  - name: site-a
    namespace: clusters
    address: gateway.apps.a.example.com
    port: "443"
    labels:
      region: us-east-2
EOF
remove_site_entry "$tmp/case6.yaml" site-a
if grep -q 'endpoints: \[\]' "$tmp/case6.yaml"; then
  echo "ok   remove last entry (literal check)"
else
  echo "FAIL remove last entry (literal check)"
  cat "$tmp/case6.yaml"
  fail=1
fi
if [[ "$(yq '.endpoints | type' "$tmp/case6.yaml")" == "!!seq" ]]; then
  echo "ok   remove last entry (type check)"
else
  echo "FAIL remove last entry (type check)"
  yq '.endpoints | type' "$tmp/case6.yaml"
  fail=1
fi

exit $fail
