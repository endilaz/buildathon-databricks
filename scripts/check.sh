#!/bin/sh
# Correctness gate: the 36 fixtures, the 1M-event dev answer, and (optionally)
# a reference output for a larger dataset.
# Usage: PY=python-with-polars STARTER=starter-dir BENCH=bench-dir scripts/check.sh [BINARY]
set -eu
BIN=$(cd "$(dirname "${1:-./shopping-sessions}")" && pwd)/$(basename "${1:-./shopping-sessions}")
PY=${PY:-python3}; STARTER=${STARTER:?}; BENCH=${BENCH:?}
OUT=$(mktemp -d); trap 'rm -rf "$OUT"' EXIT
"$PY" "$STARTER/scripts/run_fixtures.py" "$STARTER/fixtures" -- "$BIN" | tail -1
SHOPPING_WIDE=1 "$PY" "$STARTER/scripts/run_fixtures.py" "$STARTER/fixtures" -- "$BIN" | tail -1 | sed 's/^/wide: /'
SHOPPING_WIDE=1 NCPU=8 "$BIN" "$BENCH/dev" "$OUT/dev-wide" && "$PY" "$STARTER/scripts/compare.py" "$OUT/dev-wide" "$BENCH/dev-answer" | tail -1 | sed 's/^/wide dev: /'
NCPU=8 "$BIN" "$BENCH/dev" "$OUT/dev" && "$PY" "$STARTER/scripts/compare.py" "$OUT/dev" "$BENCH/dev-answer" | tail -1
SHOPPING_MMAP=1 NCPU=8 "$BIN" "$BENCH/dev" "$OUT/dev-mmap" && "$PY" "$STARTER/scripts/compare.py" "$OUT/dev-mmap" "$BENCH/dev-answer" | tail -1 | sed 's/^/mmap dev: /'
# undefined behaviour (misaligned references crash on x86 but not under emulation)
make -s -C "$(dirname "$0")/.." ubsan
UB="$(cd "$(dirname "$0")/.." && pwd)/shopping-sessions-ubsan"
"$PY" "$STARTER/scripts/run_fixtures.py" "$STARTER/fixtures" -- "$UB" | tail -1 | sed 's/^/ubsan: /'
SHOPPING_WIDE=1 NCPU=8 "$UB" "$BENCH/dev" "$OUT/dev-ub" && "$PY" "$STARTER/scripts/compare.py" "$OUT/dev-ub" "$BENCH/dev-answer" | tail -1 | sed 's/^/ubsan wide dev: /'
NCPU=8 "$UB" "$BENCH/dev" "$OUT/dev-ub2" && "$PY" "$STARTER/scripts/compare.py" "$OUT/dev-ub2" "$BENCH/dev-answer" | tail -1 | sed 's/^/ubsan dev: /'
if [ -d "$BENCH/practice-output" ]; then
  NCPU=8 "$BIN" "$BENCH/practice" "$OUT/practice" && "$PY" "$STARTER/scripts/compare.py" "$OUT/practice" "$BENCH/practice-output" | tail -1
fi
