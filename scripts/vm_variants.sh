#!/bin/sh
# Builds variants of the working tree in the Docker VM, one per -D flag set.
# Usage: scripts/vm_variants.sh OUTDIR NAME="FLAGS" ...   (needs the ss-dev image)
set -eu
OUT=$1; shift
SRC=$OUT/src-copy; rm -rf "$SRC"; mkdir -p "$SRC"; cp -r "$(dirname "$0")/../src" "$(dirname "$0")/../Makefile" "$SRC/"
for spec in "$@"; do
  name=${spec%%=*}; flags=${spec#*=}
  docker run --rm -v "$OUT":/out ss-dev sh -c "cd /out/src-copy && make -s CXX=g++ ARCHFLAGS=-mcpu=native \
    CXXFLAGS='-O3 -DNDEBUG -flto=auto $flags' LDFLAGS=-static BIN=/out/$name 2>&1 | grep -v snprintf | grep -E 'error' || true"
done
