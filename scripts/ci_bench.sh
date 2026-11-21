#!/bin/bash
# Interleaved timing of ./ours and ./refbin on DATA, under both THP modes,
# plus perf stat counters and perf profiles. Usage: ci_bench.sh DATA RESULTS
set -u
D=$1; OUT=$2; NC=$(nproc)
{
  echo "== machine"
  lscpu | grep -E "Model name|^CPU\(s\)|Thread|Core|L1d|L2|L3|NUMA node\(s\)"
  echo "avx2: $(grep -qw avx2 /proc/cpuinfo && echo yes || echo no); avx512bw: $(grep -qw avx512bw /proc/cpuinfo && echo yes || echo no)"
  echo "kernel $(uname -r); THP $(cat /sys/kernel/mm/transparent_hugepage/enabled); defrag $(cat /sys/kernel/mm/transparent_hugepage/defrag)"
  free -g | head -2
} >> "$OUT"
cat $(find "$D" -type f) > /dev/null
run() {  # name binary
  rm -rf /tmp/o; mkdir -p /tmp/o
  s=$(date +%s%N)
  SHOPPING_PROFILE=1 NCPU=$NC /usr/bin/time -f "time: user=%U sys=%S maxrss=%MKB minflt=%R" "$2" "$D" /tmp/o 2>/tmp/log
  e=$(date +%s%N)
  printf "%-5s wall=%5sms  " "$1" $(( (e-s)/1000000 ))
  awk '/ms\]/{printf "%s=%s ", $4, $5} /^time:/{printf " %s %s %s %s", $2, $3, $4, $5}' /tmp/log; echo
}
for thp in madvise always; do
  echo $thp | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null
  echo "== THP=$thp NCPU=$NC" >> "$OUT"
  for i in 1 2 3 4 5 6; do run ours ./ours >> "$OUT"; run ref ./refbin >> "$OUT"; done
done
echo madvise | sudo tee /sys/kernel/mm/transparent_hugepage/enabled > /dev/null
# hardware counters and profiles (may be unavailable in the VM)
sudo apt-get install -y -qq linux-tools-common "linux-tools-$(uname -r)" > /dev/null 2>&1 || true
if command -v perf > /dev/null; then
  echo "== perf stat (THP=madvise)" >> "$OUT"
  for b in ours-g refbin; do
    echo "-- $b" >> "$OUT"
    rm -rf /tmp/o; sudo perf stat -e task-clock,cycles,instructions,branch-misses,cache-misses,dTLB-load-misses,page-faults \
      ./$b "$D" /tmp/o 2>&1 >/dev/null | grep -E "task-clock|cycles|instructions|misses|faults|elapsed" >> "$OUT"
  done
  for b in ours-g refbin; do
    rm -rf /tmp/o; sudo perf record -q -F 2000 -g -o /tmp/$b.data ./$b "$D" /tmp/o > /dev/null 2>&1
    sudo perf report -i /tmp/$b.data --no-children --stdio --percent-limit 0.7 -g none 2>/dev/null | grep -v "^#" | grep -v "^$" | cut -c1-200 > perf-$b.txt
  done
  rm -rf /tmp/o; sudo perf record -q -F 4000 -o /tmp/ann.data ./ours-g "$D" /tmp/o > /dev/null 2>&1
  sudo perf annotate -i /tmp/ann.data --stdio 2>/dev/null | head -60000 > perf-annotate-ours.txt
else
  echo "perf unavailable" >> "$OUT"
fi
cat "$OUT"
