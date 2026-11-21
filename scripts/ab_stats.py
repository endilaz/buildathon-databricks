#!/usr/bin/env python3
"""Medians per binary from vm_bench.sh output on stdin: wall and every stage."""
import re, statistics, sys
from collections import defaultdict
runs = defaultdict(lambda: defaultdict(list))
names = sys.argv[1:]
for line in sys.stdin:
    m = re.match(r"bin(\d+) wall=\s*(\d+)ms\s+(.*)", line)
    if not m:
        continue
    key = names[int(m[1]) - 1] if len(names) >= int(m[1]) else "bin" + m[1]
    runs[key]["wall"].append(float(m[2]))
    for stage, ms in re.findall(r"(\w+)=([\d.]+)", m[3]):
        runs[key][stage].append(float(ms))
stages = list(dict.fromkeys(s for r in runs.values() for s in r))
print(f"{'':10}" + "".join(f"{s:>10}" for s in stages) + "   n")
for key, r in runs.items():
    print(f"{key:10}" + "".join(f"{statistics.median(r[s]):10.0f}" if r.get(s) else f"{'-':>10}" for s in stages)
          + f"   {len(r['wall'])}")
