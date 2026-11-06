#!/usr/bin/env bash
set -euo pipefail

if [[ "$#" -ne 1 ]]; then
  echo "Usage: $0 <grid-gateway-binary>" >&2
  exit 2
fi

binary="$1"
if [[ ! -f "$binary" ]]; then
  echo "FIPS gateway binary does not exist: $binary" >&2
  exit 2
fi

for tool in strings readelf; do
  if ! command -v "$tool" >/dev/null 2>&1; then
    echo "Required tool is not installed: $tool" >&2
    exit 2
  fi
done

if strings -a -- "$binary" | grep -E 'aws_lc_|AWS-LC' >/dev/null; then
  echo "FIPS gateway binary contains forbidden AWS-LC marker: $binary" >&2
  exit 1
fi

dynamic_section="$(readelf -d "$binary")"
for library in libcrypto.so.3 libssl.so.3; do
  if ! grep -Fq "Shared library: [$library]" <<<"$dynamic_section"; then
    echo "FIPS gateway binary does not dynamically link $library: $binary" >&2
    exit 1
  fi
done

echo "FIPS gateway binary excludes AWS-LC markers and dynamically links OpenSSL 3: $binary"
