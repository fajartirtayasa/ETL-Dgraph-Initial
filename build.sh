#!/usr/bin/env bash
# Builds the standalone linux/arm64 binary this repository ships (no image, no ECS):
#
#   bash build.sh
#
# Output, under out/ (gitignored):
#   out/traceability-dgraph-etl     static ELF for Linux arm64 (the EC2 target)
#   out/config/app.yaml             the binary reads config/app.yaml relative to its working directory
#
# Always builds inside the pinned golang container, even when Go is installed locally, so the toolchain is the
# same everywhere. The container is the host-native image and cross-compiles. CGO_ENABLED=0 is mandatory: a
# cgo build would link the Debian 12 glibc and could fail on Amazon Linux. The module and build caches are the
# volumes parity/go.sh uses, so the two scripts share downloads.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")" && pwd)"
HOST_ROOT="$(cd "$ROOT" && (pwd -W 2>/dev/null || pwd))"

docker volume create parity-gomod >/dev/null
docker volume create parity-gocache >/dev/null

MSYS_NO_PATHCONV=1 exec docker run --rm \
  -v "$HOST_ROOT":/src \
  -v parity-gomod:/go/pkg/mod \
  -v parity-gocache:/root/.cache/go-build \
  -w /src \
  -e CGO_ENABLED=0 -e GOOS=linux -e GOARCH=arm64 \
  golang:1.25.3-bookworm sh -c '
    go build -trimpath -ldflags="-s -w" -o out/traceability-dgraph-etl . &&
    mkdir -p out/config &&
    cp config/app.yaml out/config/app.yaml'
