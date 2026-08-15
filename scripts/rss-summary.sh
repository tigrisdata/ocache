#!/usr/bin/env bash
# Summarize an rss-sample.sh CSV: peak and final VmRSS/RssAnon/VmData in MiB.
# Usage: rss-summary.sh <csv>
set -euo pipefail

CSV="${1:?usage: rss-summary.sh <csv>}"

awk -F, '
  NR > 1 {
    if ($2 > prss)  prss  = $2
    if ($3 > panon) panon = $3
    if ($4 > pdata) pdata = $4
    frss = $2; fanon = $3; fdata = $4
    n++
  }
  END {
    if (n == 0) { print "no samples recorded"; exit }
    printf "samples: %d\n", n
    printf "peak   VmRSS=%8.1f MiB   RssAnon=%8.1f MiB   VmData=%8.1f MiB\n", prss/1024,  panon/1024,  pdata/1024
    printf "final  VmRSS=%8.1f MiB   RssAnon=%8.1f MiB   VmData=%8.1f MiB\n", frss/1024,  fanon/1024,  fdata/1024
  }
' "$CSV"
