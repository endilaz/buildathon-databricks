#!/bin/sh
# CPU time (user+sys, s) per run for each binary, interleaved: steadier than
# wall time when the host is busy. Usage: scripts/vm_cpu.sh DATASET RUNS BINARY...
set -eu
DATASET=$1; RUNS=$2; shift 2
MOUNTS=""; NAMES=""; i=0
for b in "$@"; do
  i=$((i+1)); MOUNTS="$MOUNTS -v $(cd "$(dirname "$b")" && pwd)/$(basename "$b"):/bin$i:ro"; NAMES="$NAMES $i:$(basename "$b")"
done
docker run --rm --cpus 8 -e NCPU=8 -e RUNS="$RUNS" -e NAMES="$NAMES" -e D=/bench/data/$DATASET -v ssbench:/bench $MOUNTS ss-dev sh -c '
command -v /usr/bin/time >/dev/null || (apt-get update -qq && apt-get install -y -qq time) >/dev/null 2>&1
cat $(find $D -type f) > /dev/null
i=0
while [ $i -lt $RUNS ]; do
  i=$((i+1))
  for nb in $NAMES; do
    n=${nb%%:*}; name=${nb#*:}; rm -rf /tmp/o
    /usr/bin/time -f "$name %U %S %e" /bin$n $D /tmp/o 2>&1 >/dev/null | tail -1
  done
done' | awk '{u[$1]+=$2; s[$1]+=$3; w[$1]+=$4; n[$1]++} END {for (k in u) printf "%-14s cpu %.2f s (user %.2f sys %.2f)  wall %.2f s  n=%d\n", k, (u[k]+s[k])/n[k], u[k]/n[k], s[k]/n[k], w[k]/n[k], n[k]}'
