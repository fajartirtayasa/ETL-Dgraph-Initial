#!/usr/bin/env bash
# Runs a go command from the repository root, inside the pinned toolchain
# container when no local Go is installed (or PARITY_USE_DOCKER=1):
#
#   parity/go.sh test ./...
#   parity/go.sh run ./parity/fixture/gen
#   parity/go.sh test ./internal/etl/incremental -run Parity -args -update
#
# L3 tests read PARITY_PG_URL. Inside the container it defaults to the
# fixture started by parity/fixture/rebuild.sh, reached via the host gateway.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if command -v go >/dev/null 2>&1 && [ -z "${PARITY_USE_DOCKER:-}" ]; then
  cd "$ROOT"
  exec go "$@"
fi

HOST_ROOT="$(cd "$ROOT" && (pwd -W 2>/dev/null || pwd))"
# The container's git must read the checkout with the host's line-ending
# setting, or a Windows CRLF checkout looks modified to the provenance guard.
AUTOCRLF="$(git -C "$ROOT" config --get core.autocrlf 2>/dev/null || echo false)"
docker volume create parity-gomod >/dev/null
docker volume create parity-gocache >/dev/null

MSYS_NO_PATHCONV=1 exec docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -v "$HOST_ROOT":/src \
  -v parity-gomod:/go/pkg/mod \
  -v parity-gocache:/root/.cache/go-build \
  -w /src \
  -e PARITY_PG_URL="${PARITY_PG_URL:-postgres://postgres@host.docker.internal:55432/parity_fixture?sslmode=disable}" \
  -e PARITY_REQUIRE_L3="${PARITY_REQUIRE_L3:-}" \
  -e GIT_CONFIG_COUNT=2 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
  -e GIT_CONFIG_KEY_1=core.autocrlf -e GIT_CONFIG_VALUE_1="$AUTOCRLF" \
  golang:1.25.3-bookworm go "$@"
