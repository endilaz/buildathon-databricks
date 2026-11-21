#!/bin/sh
# Interleaved timing of Linux builds inside the Docker VM, on data already
# copied into the `ssbench` volume. Usage: scripts/vm_bench.sh DATASET RUNS BINARY...
# The page cache is warmed first; prints each run's wall time and stage timings.
set -eu
DATASET=$1; RUNS=$2; shift 2
MOUNTS=""; NAMES=""; i=0
for b in "$@"; do
  i=$((i+1)); MOUNTS="$MOUNTS -v $(cd "$(dirname "$b")" && pwd)/$(basename "$b"):/bin$i:ro"; NAMES="$NAMES $i"
done
docker run --rm --cpus 8 -e NCPU=8 -e RUNS="$RUNS" -e NAMES="$NAMES" -e D=/bench/data/$DATASET \
  -v ssbench:/bench $MOUNTS debian:bookworm-slim sh -c '
cat $(find $D -type f) > /dev/null
i=0
while [ $i -lt $RUNS ]; do
  i=$((i+1))
  for n in $NAMES; do
    rm -rf /tmp/o; mkdir -p /tmp/o
    s=$(date +%s%N); SHOPPING_PROFILE=1 /bin$n $D /tmp/o 2>/tmp/log; e=$(date +%s%N)
    printf "bin%s wall=%5sms  " $n $(( (e-s)/1000000 ))
    awk "/ms\\]/{printf \"%s=%s \", \$4, \$5}" /tmp/log; echo
  done
done'
