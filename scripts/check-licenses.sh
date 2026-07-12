#!/usr/bin/env bash
#
# check-licenses.sh — fail if any compiled-in dependency uses a license that is
# not on the allowlist. Guards against a copyleft (GPL/AGPL/LGPL) or otherwise
# incompatible dependency sneaking into OCache's shipped module tree.
#
# It classifies the modules actually imported by OCache's non-test packages
# (via `go list -deps`), reading each module's LICENSE file from the module
# cache. Requires the dependency sources to be present in the cache (true after
# a build) and the cgo build env to be set for the RocksDB-linked modules; run
# it via `make license-check`, or in CI after the build step.
#
set -uo pipefail
cd "$(dirname "$0")/.."

# SPDX-ish identifiers we accept. MPL-2.0 is weak (file-level) copyleft and is
# used unmodified — allowed. Anything not here (GPL/AGPL/LGPL/BSL/unknown) fails.
ALLOWED="${ALLOWED:-Apache-2.0 MIT BSD ISC MPL-2.0 Public-Domain}"

MODULE_DIRS="./server/... ./client/... ./embedded/... ./storage/... ./coordinator/... ./common/... ./proto/..."

classify() {
  local dir="$1" f c
  [ -z "$dir" ] && { echo "UNKNOWN(no-source)"; return; }
  f=$(ls "$dir" 2>/dev/null | grep -iE '^(LICEN[CS]E|COPYING|LICENSE-APACHE|LICENSE-MIT|UNLICENSE)(\.(md|txt|rst))?$' | head -1)
  [ -z "$f" ] && { echo "UNKNOWN(no-license-file)"; return; }
  c=$(tr -d '\r' < "$dir/$f")
  # MPL/BSL first: MPL-2.0 text references the GNU licenses in its exhibit.
  if   grep -qi "Mozilla Public License"        <<<"$c"; then echo "MPL-2.0"
  elif grep -qi "Business Source License"       <<<"$c"; then echo "BSL"
  elif grep -qi "AFFERO GENERAL PUBLIC LICENSE" <<<"$c"; then echo "AGPL"
  elif grep -qi "LESSER GENERAL PUBLIC LICENSE" <<<"$c"; then echo "LGPL"
  elif grep -qi "GNU GENERAL PUBLIC LICENSE"    <<<"$c"; then echo "GPL"
  elif grep -qi "Apache License"                <<<"$c"; then echo "Apache-2.0"
  elif grep -qi "ISC License"                   <<<"$c"; then echo "ISC"
  elif grep -qi "Redistribution and use in source and binary" <<<"$c"; then echo "BSD"
  elif grep -qi "MIT License" <<<"$c" || grep -qi "Permission is hereby granted, free of charge" <<<"$c"; then echo "MIT"
  elif grep -qi "creativecommons.org/publicdomain/zero\|Unlicense\|public domain" <<<"$c"; then echo "Public-Domain"
  else echo "OTHER"; fi
}

echo "Resolving compiled-in dependency modules..."
mods=$(go list -deps -f '{{with .Module}}{{.Path}}{{end}}' $MODULE_DIRS 2>/dev/null \
        | grep -v '^github.com/tigrisdata/ocache' | grep . | sort -u)
if [ -z "$mods" ]; then
  echo "ERROR: could not resolve dependency modules (is the cgo build env set and sources built?)." >&2
  exit 2
fi

violations=0
count=0
while IFS= read -r mod; do
  count=$((count + 1))
  dir=$(go list -m -f '{{.Dir}}' "$mod" 2>/dev/null)
  lic=$(classify "$dir")
  if ! grep -qw -- "$lic" <<<"$ALLOWED"; then
    printf '  DISALLOWED  %-12s %s\n' "$lic" "$mod"
    violations=$((violations + 1))
  fi
done <<<"$mods"

echo "Checked $count compiled-in modules against allowlist: $ALLOWED"
if [ "$violations" -gt 0 ]; then
  echo "FAIL: $violations dependency module(s) use a non-allowlisted license." >&2
  echo "If a new license is intentional and acceptable, add it to ALLOWED in scripts/check-licenses.sh." >&2
  exit 1
fi
echo "OK: all compiled-in dependencies are allowlisted."
