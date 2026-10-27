#!/usr/bin/env bash
# Runs a go command from the repository root, inside the pinned toolchain
# container when no local Go is installed (or PARITY_USE_DOCKER=1):
#
#   parity/go.sh test ./...
#   parity/go.sh run ./parity/fixture/gen
#   parity/go.sh test ./internal/etl/initial -run TestEmittedPredicates -v
#
# The schema tests read the canonical schema.dql from the sibling ../ETL-Dgraph-Incremental (C7), so that
# repository must sit beside this one; the script stops if it does not. In the container both repositories are
# mounted side by side as /work/<repo> (this one read-write, the sibling read-only), working directory
# /work/ETL-Dgraph-Initial, so the same relative path resolves there.
#
# The stamping rehearsal (internal/etl/initial/stamp_rehearsal_test.go) reads
# PARITY_PG_URL and clones that database; inside the container it defaults to
# the fixture started by parity/fixture/rebuild.sh, reached via the host
# gateway. PARITY_REQUIRE_L3=1 turns its skip (no database) into a failure.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"

if [ ! -d "$ROOT/../ETL-Dgraph-Incremental" ]; then
  echo "parity/go.sh: ETL-Dgraph-Incremental not found at $(cd "$ROOT/.." && pwd)/ETL-Dgraph-Incremental" >&2
  echo "  the tests compare the export with its canonical schema.dql: place that repository beside this one." >&2
  exit 2
fi

if command -v go >/dev/null 2>&1 && [ -z "${PARITY_USE_DOCKER:-}" ]; then
  cd "$ROOT"
  exec go "$@"
fi

HOST_ROOT="$(cd "$ROOT" && (pwd -W 2>/dev/null || pwd))"
HOST_SIBLING="$(cd "$ROOT/../ETL-Dgraph-Incremental" && (pwd -W 2>/dev/null || pwd))"
# The container's git must read the checkout with the host's line-ending
# setting, or a Windows CRLF checkout looks modified to the provenance guard.
AUTOCRLF="$(git -C "$ROOT" config --get core.autocrlf 2>/dev/null || echo false)"
docker volume create parity-gomod >/dev/null
docker volume create parity-gocache >/dev/null

MSYS_NO_PATHCONV=1 exec docker run --rm \
  --add-host=host.docker.internal:host-gateway \
  -v "$HOST_ROOT":/work/ETL-Dgraph-Initial \
  -v "$HOST_SIBLING":/work/ETL-Dgraph-Incremental:ro \
  -v parity-gomod:/go/pkg/mod \
  -v parity-gocache:/root/.cache/go-build \
  -w /work/ETL-Dgraph-Initial \
  -e PARITY_PG_URL="${PARITY_PG_URL:-postgres://postgres@host.docker.internal:55432/parity_fixture?sslmode=disable}" \
  -e PARITY_REQUIRE_L3="${PARITY_REQUIRE_L3:-}" \
  -e GIT_CONFIG_COUNT=2 -e GIT_CONFIG_KEY_0=safe.directory -e GIT_CONFIG_VALUE_0='*' \
  -e GIT_CONFIG_KEY_1=core.autocrlf -e GIT_CONFIG_VALUE_1="$AUTOCRLF" \
  golang:1.25.3-bookworm go "$@"
