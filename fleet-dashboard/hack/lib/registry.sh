#!/usr/bin/env bash
# SPDX-License-Identifier: Apache-2.0
# Shared registry (clusters.yaml) manipulation for register-site.sh and unregister-site.sh.
# Requires mikefarah yq v4. Source this file; do not execute it.

require_yq() {
  if ! command -v yq >/dev/null 2>&1 || ! yq --version 2>&1 | grep -q 'mikefarah'; then
    cat >&2 <<'EOF'
yq v4 (https://github.com/mikefarah/yq) is required and was not found.
Install:
  curl -fsSL https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o ~/.local/bin/yq && chmod +x ~/.local/bin/yq
  (Fedora: sudo dnf install yq)
Note: the Python "yq" wrapper is a different tool and will not work.
EOF
    return 1
  fi
}

# merge_site_entry <clusters-file> <entry-file>
# Replaces the endpoints[] item whose .name equals the entry's .name, in place,
# keeping labels the existing item has that the entry does not set. Appends when absent.
merge_site_entry() {
  local clusters="$1" entry="$2"
  local name tmpdir
  name=$(yq -r '.name' "$entry")
  [[ -n "$name" && "$name" != "null" ]] || { echo "merge_site_entry: entry has no .name" >&2; return 1; }

  if [[ ! -s "$clusters" ]]; then
    printf 'endpoints: []\n' >"$clusters"
  fi
  yq -i '.endpoints = (.endpoints // [])' "$clusters"

  tmpdir=$(mktemp -d)
  trap 'rm -rf "$tmpdir"' RETURN
  local count
  count=$(SITE_NAME="$name" yq '[.endpoints[] | select(.name == strenv(SITE_NAME))] | length' "$clusters")
  if [[ "$count" -gt 0 ]]; then
    SITE_NAME="$name" yq '.endpoints[] | select(.name == strenv(SITE_NAME)) | .labels // {}' "$clusters" >"$tmpdir/old-labels.yaml"
    cp "$entry" "$tmpdir/entry.yaml"
    OLD_LABELS="$tmpdir/old-labels.yaml" yq -i '.labels = ((load(strenv(OLD_LABELS)) // {}) * (.labels // {}))' "$tmpdir/entry.yaml"
    SITE_NAME="$name" ENTRY_FILE="$tmpdir/entry.yaml" \
      yq -i '(.endpoints[] | select(.name == strenv(SITE_NAME))) = load(strenv(ENTRY_FILE))' "$clusters"
  else
    ENTRY_FILE="$entry" yq -i '.endpoints += [load(strenv(ENTRY_FILE))]' "$clusters"
  fi
}

# remove_site_entry <clusters-file> <site-name>
remove_site_entry() {
  local clusters="$1" name="$2"
  [[ -s "$clusters" ]] || return 0
  SITE_NAME="$name" yq -i 'del(.endpoints[] | select(.name == strenv(SITE_NAME)))' "$clusters"
}
