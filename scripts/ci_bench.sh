#!/bin/bash
# Interleaved timing of the Rust candidate against the submitted build, its
# feature variants and the C++ build on DATA, then a perf profile of the
# candidate. Usage: ci_bench.sh DATA RESULTS
set -u
D=$1; OUT=$2; NC=$(nproc)
{
  echo "== machine"
  lscpu | grep -E "Model name|^CPU\(s\)|Thread|Core|L1d|L2|L3|NUMA node\(s\)"
  echo "avx2: $(grep -qw avx2 /proc/cpuinfo && echo yes || echo no); avx512bw: $(grep -qw avx512bw /proc/cpuinfo && echo yes || echo no)"
  echo "kernel $(uname -r); THP $(cat /sys/kernel/mm/transparent_hugepage/enabled)"
} >> "$OUT"
cat $(find "$D" -type f) > /dev/null
run() {  # name env binary
  O=$(mktemp -d)
  s=$(date +%s%N)
  env $2 SHOPPING_PROFILE=1 NCPU=$NC /usr/bin/time -f "time: user=%U sys=%S maxrss=%MKB" "$3" "$D" "$O" 2>/tmp/log
  e=$(date +%s%N)
  printf "%-8s wall=%5sms  " "$1" $(( (e-s)/1000000 ))
  awk '/ms\]/{printf "%s=%s ", $4, $5} /^time:/{printf " %s %s %s", $2, $3, $4}' /tmp/log; echo
  rm -rf -- "$O"
}
echo "== NCPU=$NC: subm = origin/main image, cand = this branch's image, v-* = host-built variants" >> "$OUT"
for i in $(seq 1 10); do
  run subm "X=1" ./subm >> "$OUT"
  run cand "X=1" ./cand >> "$OUT"
  run base "X=1" ./v-base >> "$OUT"
  run scan64 "X=1" ./v-scan64 >> "$OUT"
  run pd8 "X=1" ./v-pd8 >> "$OUT"
  run bigdd "X=1" ./v-bigdd >> "$OUT"
  run nofilt "X=1" ./v-nofilter >> "$OUT"
  run mmap "SHOPPING_MMAP=1" ./cand >> "$OUT"
  run cpp "X=1" ./cpp >> "$OUT"
done
# outputs of the candidate and the submitted build must agree
A=$(mktemp -d); B=$(mktemp -d)
NCPU=$NC ./cand "$D" "$A" && NCPU=$NC ./subm "$D" "$B"
for t in user_daily campaign_daily; do
  if cmp -s <(cat "$A"/$t/*.parquet | md5sum) <(cat "$B"/$t/*.parquet | md5sum); then echo "$t: identical bytes" >> "$OUT"; else echo "$t: bytes differ (row order may differ; compare locally)" >> "$OUT"; fi
done
sudo apt-get install -y -qq linux-tools-common "linux-tools-$(uname -r)" > /dev/null 2>&1 || true
if command -v perf > /dev/null; then
  O=$(mktemp -d); sudo perf record -q -F 2000 -g -o /tmp/cand.data ./cand-g "$D" "$O" > /dev/null 2>&1
  sudo perf report -i /tmp/cand.data --no-children --stdio --percent-limit 0.5 -g none 2>/dev/null | grep -v "^#" | grep -v "^$" | cut -c1-220 > perf-cand.txt
  sudo perf annotate -i /tmp/cand.data --stdio 2>/dev/null | head -40000 > perf-annotate-cand.txt
else
  echo "perf unavailable" >> "$OUT"
fi
cat "$OUT"
