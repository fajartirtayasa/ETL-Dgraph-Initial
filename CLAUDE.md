# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-binary Go ETL (`hara/traceability-dgraph-etl`) that exports traceability data (events and the links between them) from PostgreSQL, at a scale of millions of rows, as gzipped, chunked RDF N-Quad files in `./initial_imports/`, for the Dgraph bulk loader to consume out of band. It does **not** connect to Dgraph.

It accepts exactly one mode: **`APP_MODE=initial`** (or `app.mode: initial`). Anything else, including an empty value, is rejected at startup, before PostgreSQL is contacted (see Configuration flow). The daily incremental sync that used to live here is now the Python ETL in the sibling repository `ETL-Dgraph-Incremental`.

Three repositories sit side by side as sibling folders, and this one needs the others:

| Folder | Role |
|---|---|
| `ETL-Dgraph-Initial` | this repository: the Go export |
| `ETL-Dgraph-Incremental` | the Python incremental ETL; owns the canonical `schema.dql`; reads this repository's synthetic fixture in place |
| `Dgraph` | holds the live `REFACTOR_PLAN.md` and `REFACTOR_ASSESSMENT.md` |

`command.txt` is the original requirements brief from the repo owner (both strategies, field mappings, technical constraints). It is kept unchanged because the Python repository relies on it for field mappings; only the initial strategy applies to this code. Read it before changing export semantics. The owner maintains technical flow documentation separately — do not generate flow/architecture docs unless asked.

`BASE_KNOWLEDGE.md` is a historical snapshot at commit `b487294` (it still describes the incremental mode and the ECS deployment). Do not treat it as current.

## Commands

Everything goes through Docker, targeting the pinned `golang:1.25.3-bookworm` image; no local Go is needed.

```bash
bash build.sh                                # out/traceability-dgraph-etl (linux/arm64, static) + out/config/app.yaml
parity/go.sh test ./...                      # the whole suite
parity/go.sh test ./internal/etl -run TestParseMode -v     # a single test
parity/go.sh vet ./...
parity/go.sh run ./parity/fixture/gen        # regenerate the synthetic fixture (a reproducibility check, see below)
```

- `build.sh` always builds in the container, even when Go is installed locally, with `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 -trimpath -ldflags="-s -w"`. `out/` is gitignored.
- `parity/go.sh` runs `go` locally if it is installed, unless `PARITY_USE_DOCKER=1` forces the container. In the container it mounts this repository (read-write) and `../ETL-Dgraph-Incremental` (read-only) as `/work/<repo>`, and stops with an error if the sibling is missing.
- **The tests need `ETL-Dgraph-Incremental` beside this repository**: the schema tests read its `schema.dql` and **fail, never skip,** when it is absent.
- The binary resolves config relative to the working directory (`config/app.yaml`, see `internal/app/consts/path.go`), so run it from the repo root, or from a directory that holds `config/` (which is what `out/` is).

`setup/` holds old Dgraph host-provisioning scripts. They are historical, unmaintained and not part of the app lifecycle (see `setup/README.md`).

## Configuration flow

`config/app.yaml` is loaded first, then every field is overridden by an env var in `overrideWithEnv` (`internal/app/config.go`); `.env` is loaded at startup via godotenv if present. `.env.example` lists the variables.

Adding one tunable touches three files, which must stay in sync:

1. `config/app.yaml` — yaml default + inline comment
2. `internal/app/config.go` — struct field *and* the matching `util.GetEnvAs*` line
3. `.env.example`

`main.go` deliberately re-maps `app.*Config` structs into package-local config types (`database.Config`) so `internal/etl/**` never imports `internal/app`.

**`APP_MODE` is strict.** `etl.ParseMode` trims whitespace, ignores case and accepts only `initial`. An empty or whitespace-only value, `incremental` (with its own message pointing at ETL-Dgraph-Incremental) and anything else are errors, and `main.go` parses the mode right after loading the config, before the Postgres connection. A blank `APP_MODE=` line in `.env` **overrides** the yaml with an empty value and fails, which is why `.env.example` ships `APP_MODE=initial`. A yaml without an `app:` section gets an empty `AppConfig`, so the mode error appears instead of a nil-pointer panic. `INITIAL_WORKER_COUNT` and `INITIAL_CHUNK_SIZE` tune the export (blank means the default).

## Architecture

`main.go` wires dependencies only (config → mode check → slog JSON logger → Postgres adapter → `etl.Run`). `internal/etl/runner.go` rejects any mode other than `initial` and builds the migrator.

### `internal/etl/common` — shared RDF machinery

- `BlankNode(prefix, value)` produces deterministic blank-node IDs. Non-alphanumeric values are sanitised *and* suffixed with a SHA-1 of the original so `"A-1"` and `"A_1"` cannot collapse. The two output files are joined by it, so changing it breaks the join and invalidates any already-loaded graph.
- `StringInterner` — 32-shard, RWMutex-per-shard, safe for concurrent use. Used to collapse repeated column values (product ids, units, subject types) across millions of rows.
- `ChunkedGzipRDFWriter` — owns its file in a single background goroutine fed by a channel; rolls over to `base_1.rdf.gz`, `base_2.rdf.gz`, … every `chunk_size` records (default 1M).

### `internal/etl/initial` — RDF export

Pipeline: one fetcher goroutine streams `eventsQuery` into a buffered channel → `WorkerCount` aggregator workers build RDF per row (reusing `strings.Builder`s from a `sync.Pool`) → chunked writer. Managed by `errgroup`, so the first error cancels the whole fan-out. Links are streamed single-threaded *after* the event writers close, into `links.rdf.gz`.

`Run` **deletes `initial_imports/` wholesale** at the start of every run.

Event nodes and link edges are joined purely by blank-node identity across the two files — `links.rdf.gz` references `_:event_<id>` subjects that `events.rdf.gz` defines.

### `pkg/` — reusable infrastructure

Interface-first (`contract.go` declares the `Adapter`, a sibling file implements it) so ETL code depends on `database.Adapter`, never on a concrete client. `pkg/aws` (S3) and `pkg/database/migration.go` (goose) are wired into config but not used by any ETL path; `db_clean_replica`, `pkg/validator` and `pkg/httpx` are likewise unused.

## Schema

**`schema.dql` must stay byte-identical to `../ETL-Dgraph-Incremental/schema.dql`.** The Python repository is canonical, and the bulk-load runbook uses Python's copy. Never edit this copy on its own: change the canonical one, then copy it here. `.gitattributes` pins LF line endings so the comparison is not broken by a Windows checkout. Two tests in `internal/etl/initial/schema_test.go` enforce it:

- a mirror test: the two files are byte-identical (the first differing line is reported);
- a predicate test: the predicates and scalar types the export actually emits (events through `newAggregator` / `emitEvent`, links through `streamLinks` over a stub SQL driver) equal the schema's scalar declarations, which equal the fields of `type Event`, in both directions, plus `links` as `[uid]`. A new column therefore fails the test until the canonical schema declares it.

`schema.dql` is applied to the cluster **manually**, never loaded by the binary, so adding a predicate in Go also means pushing the schema by hand.

`denorm_schema.dql` is an alternative normalised model (`event.*` predicates, separate `Client`/`Plant`/`Subject`/`Mill` types). No code path references it; treat it as a design exploration, not as the deployed schema.

## The frozen fixture

`parity/fixture/` (synthetic Postgres rows, `graph_seed.json`, `scenarios.json`, `compose.yaml`, `rebuild.sh`) is **frozen**: the Python repository runs it in place and pins the same six SHA-256 values in its `tests/fixture.lock`. A checksum guard in `parity/guards/guards_test.go` (`TestGuardE_…`, with a negative test) fails on any change. Do not edit these files — not even a comment. `parity/fixture/gen` is kept as a reproducibility check: `TestGeneratedFilesUpToDate` fails if the generator would produce different bytes. The golden corpus that used to live in `parity/golden/` was removed; it survives in the Python repository's `tests/golden/` and at commit `b487294`. See `parity/README.md`.

`parity/guards/guards_test.go` also holds the synthetic-data and credential guards (no tracked dumps, credentials or real-looking data), all ordinary tests in `go test ./...`.

## Deployment

There is none. This repository is not deployed to ECS and has no Dockerfile or CI pipeline. The output is the standalone `linux/arm64` binary from `build.sh`, run ad hoc on Linux arm64 (EC2); where it runs and how `initial_imports/` reaches the bulk-load host are open operational items tracked in the Dgraph repository's plan. The Go incremental image in ECR (with `td-go-rollback.json`) is the rollback artefact for the old incremental job and is unaffected by anything here.
