# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-binary Go ETL (`hara/traceability-dgraph-etl`) that migrates traceability data (events and the links between them) from PostgreSQL into Dgraph, at a scale of millions of rows. One process run executes exactly one of two mutually exclusive modes, selected by `APP_MODE` / `app.mode`:

- **`initial`** — bulk export. Does **not** connect to Dgraph. Streams Postgres and writes gzipped, chunked RDF N-Quad files into `./initial_imports/` for the Dgraph bulk loader to consume out of band.
- **`incremental`** — daily CDC sync. Connects to Dgraph and applies live mutations.

`command.txt` is the original requirements brief from the repo owner (both strategies, field mappings, technical constraints). It is the source of truth for intended behaviour; read it before changing ETL semantics. The owner maintains technical flow documentation separately — do not generate flow/architecture docs unless asked.

## Commands

The Go toolchain is not on `PATH` in the current Windows dev environment; either install Go 1.25.3+ or build through Docker.

```bash
go build -o traceability-dgraph-etl ./main.go
go run ./main.go                      # mode comes from APP_MODE / config/app.yaml
go test ./...
go test ./pkg/util/ -run TestParseDate -v    # single test
go vet ./...

docker build -t traceability-dgraph-etl .    # root Dockerfile; distroless, bakes config/ + schema.dql
```

The binary resolves config relative to the working directory (`config/app.yaml`, see `internal/app/consts/path.go`), so run it from the repo root.

Dgraph cluster provisioning lives in `scripts/` (`setup_dgraph_zero.sh`, `setup_dgraph_alpha.sh <ZERO_IP>`, `swap.sh`) — these are one-shot EC2 host-tuning scripts, not part of the app lifecycle.

## Configuration flow

`config/app.yaml` is loaded first, then every field is overridden by an env var in `overrideWithEnv` (`internal/app/config.go`); `.env` is loaded at startup via godotenv if present.

Adding one tunable touches several files, all of which must stay in sync:

1. `config/app.yaml` — yaml default + inline comment
2. `internal/app/config.go` — struct field *and* the matching `util.GetEnvAs*` line
3. `.env.example`
4. `deployment/pipelines/stages/build.yml` — the generated `.env` content (the CI writes env vars into a `.env` baked into the image)
5. For incremental knobs only: `internal/etl/incremental/config.go` (the runtime `Config` + its `ApplyDefaults` fallback) and the `toIncrementalConfig` mapper in `main.go`

Note that `main.go` deliberately re-maps `app.*Config` structs into package-local config types (`database.Config`, `dgraph.Config`, `incremental.Config`) so `internal/etl/**` never imports `internal/app`.

## Architecture

`main.go` wires dependencies only (config → slog JSON logger → Postgres adapter → Dgraph adapter *if incremental* → `etl.Run`). `internal/etl/runner.go` dispatches on `Mode` and constructs the right migrator. Everything below that is mode-specific.

### `internal/etl/common` — shared RDF machinery

- `BlankNode(prefix, value)` produces deterministic blank-node IDs. Non-alphanumeric values are sanitised *and* suffixed with a SHA-1 of the original so `"A-1"` and `"A_1"` cannot collapse. Both initial-mode files and incremental-mode new nodes depend on this being stable — changing it invalidates any already-loaded graph.
- `StringInterner` — 32-shard, RWMutex-per-shard, safe for concurrent use. Used to collapse repeated column values (product ids, units, subject types) across millions of rows.
- `ChunkedGzipRDFWriter` — owns its file in a single background goroutine fed by a channel; rolls over to `base_1.rdf.gz`, `base_2.rdf.gz`, … every `chunk_size` records (default 1M).

### `internal/etl/initial` — RDF export

Pipeline: one fetcher goroutine streams `eventsQuery` into a buffered channel → `WorkerCount` aggregator workers build RDF per row (reusing `strings.Builder`s from a `sync.Pool`) → chunked writer. Managed by `errgroup`, so the first error cancels the whole fan-out. Links are streamed single-threaded *after* the event writers close, into `links.rdf.gz`.

`Run` **deletes `initial_imports/` wholesale** at the start of every run.

Event nodes and link edges are joined purely by blank-node identity across the two files — `links.rdf.gz` references `_:event_<id>` subjects that `events.rdf.gz` defines.

### `internal/etl/incremental` — live sync

`Migrator.Run` executes a fixed sequence; each step is a separate file:

1. `traceability_check.go` — gate. Polls `traceability_log_daily` every 15 minutes until the upstream traceability ETL for (today − 2 days, Asia/Jakarta) is no longer `Running`. The job blocks here rather than racing upstream.
2. `removal.go` — events/links marked `status = 'invalid'`, resolve `unique_id → uid`, `<uid> * * .` delete.
3. `events.go` — events reachable from unsynced valid links, chunked by `FetchChunkSize`; per chunk: lookup uids → fetch details → upsert.
4. `links.go` — same chunking but **keyed by `event1_id`**, then `sync_time` updates.
5. `materialized_views.go` — `REFRESH MATERIALIZED VIEW` for `mv_events` (concurrently), `monitoring_cache_shipments`, `mv_shipment_list`.

Invariants worth knowing before editing this package:

- **`sync_time IS NULL` is the CDC watermark.** All source queries filter on it, and `sync_time` is only stamped (`NOW()`) in `sync_time.go` after the graph mutations succeed, which is what makes a failed run safely re-runnable. Never stamp it early.
- **Link chunks must be grouped by `event1_id`, not by link row.** `deleteExistingLinks` wipes `<event1_uid> <links> *` for every event1 in the chunk before re-adding; splitting one event1's links across two chunks makes the second chunk erase the first chunk's writes.
- **Event upsert is set-or-delete, field by field.** A present value emits a `set` triple; an empty one emits a `<s> <p> *` delete in the same mutation (`MutateWithDeletesRDF`), so a value cleared in Postgres is actually cleared in Dgraph.
- **uid resolution is cached in `uidMap`** (RWMutex map). A `unique_id` with no uid becomes a blank node; the uids Dgraph returns for those blank nodes are merged back via the `blankToID` map so later links in the same run can reference them.
- **`fanOutBatches[T]` (`helpers.go`) is the only concurrency primitive** — a producer goroutine plus N workers under an `errgroup`. Every batch size and worker count it is given comes from config; don't hardcode new ones.
- `cleanupMissingLinks` strips `links` from synced events that turned out not to be `event1` of any valid link.

### `pkg/` — reusable infrastructure

Interface-first (`contract.go` declares the `Adapter`, a sibling file implements it) so ETL code depends on `database.Adapter` / `dgraph.Adapter`, never on concrete clients. `pkg/dgraph` wraps `dgo/v250` with round-robin across multiple alphas, gzip compression, and retry/backoff. `pkg/aws` (S3) and `pkg/database/migration.go` (goose) are wired into config but not yet used by any ETL path.

## Schema

`schema.dql` is the live flat Dgraph schema — all event attributes as scalar predicates on one `Event` type plus `links: [uid] @reverse`. It is applied **manually** (copied into the image, never loaded by the binary), so adding a predicate in Go code also requires pushing the schema to the cluster by hand.

`denorm_schema.dql` is an alternative normalised model (`event.*` predicates, separate `Client`/`Plant`/`Subject`/`Mill` types). No code path references it; treat it as a design exploration, not as the deployed schema.

## Deployment

Azure Pipelines (`deployment/pipelines/azure-pipelines.yml`) triggers on `gar-dev` / `gar-stg` / `gar-prd-1`. The `Build` stage writes a `.env` from the matching variable group and publishes the whole source tree as a `DockerBuildContext` artifact; each environment stage builds `deployment/dockerfiles/Dockerfile-<env>` and pushes to ECR. The per-environment Dockerfiles copy that generated `.env` into the image — the root `Dockerfile` does not.
