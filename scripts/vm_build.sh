#!/bin/sh
# Builds the image for the Docker host's platform and copies its binary out.
# Usage: scripts/vm_build.sh OUTPUT_BINARY [IMAGE_TAG]
set -eu
TAG=${2:-shopping-sessions:dev}
docker build -q -t "$TAG" "$(dirname "$0")/.." >/dev/null
cid=$(docker create "$TAG")
docker cp -q "$cid:/shopping-sessions" "$1"
docker rm "$cid" >/dev/null
