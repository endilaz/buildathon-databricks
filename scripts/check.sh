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
if [ -d "$BENCH/practice-output" ]; then
  NCPU=8 "$BIN" "$BENCH/practice" "$OUT/practice" && "$PY" "$STARTER/scripts/compare.py" "$OUT/practice" "$BENCH/practice-output" | tail -1
fi
