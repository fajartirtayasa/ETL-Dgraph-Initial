# BASE_KNOWLEDGE.md

Reference knowledge for the `Dgraph` repository (Go module `hara/traceability-dgraph-etl`), derived from reading the source, config, schemas, and pipeline definitions.

`CLAUDE.md` in this repo holds the short operational guidance (commands, invariants to respect when editing). This document is the deeper reference: exact queries, field mappings, module responsibilities, and verified gaps. Where the two overlap, `CLAUDE.md` is the summary and this is the detail.

---

## 1. Project overview

A single-binary batch ETL that moves **traceability data** — supply-chain `events` and the directional `links` between them — from **PostgreSQL** into a **Dgraph** graph database. Target scale stated in `command.txt`: millions of rows.

One process execution runs **exactly one** of two mutually exclusive strategies, chosen by `APP_MODE` (`app.mode` in YAML), parsed in `internal/etl/mode.go`:

| Mode | Dgraph connection | Output | Entry point |
|---|---|---|---|
| `initial` (default for any unrecognised value) | **none** | gzipped chunked RDF N-Quad files in `./initial_imports/`, for the Dgraph bulk loader to ingest out of band | `internal/etl/initial.Migrator` |
| `incremental` | required | live mutations against Dgraph alphas | `internal/etl/incremental.Migrator` |

`ParseMode` defaults to `ModeInitial` — an empty or misspelled `APP_MODE` silently runs the initial export (which wipes `initial_imports/`), it does not error.

**`command.txt`** is the owner's original requirements brief for both strategies, including field mappings and the constraint *"don't create any documentation flow technical (!important, because i already have)"* — i.e. the flow documentation is maintained outside this repo. It is the behavioural source of truth; the SQL in it has since drifted from the code (see §9).

---

## 2. Directory structure

```
main.go                      Composition root: config → logger → adapters → etl.Run
config/app.yaml              YAML defaults for every tunable
.env.example                 Env var names (all override YAML)
schema.dql                   Live Dgraph schema (flat Event type) — applied MANUALLY
denorm_schema.dql            Alternative normalised schema — referenced by NO code
command.txt                  Owner's requirements brief for both ETL strategies

internal/app/                Config struct + env override layer
  consts/                    ConfigPath="config/", ConfigFile="app.yaml", plus unused api/file/http_header consts
internal/etl/
  mode.go, runner.go         Mode parsing and dispatch to the right migrator
  common/                    Shared RDF machinery (blank nodes, interner, writers, literals)
  initial/                   RDF file export pipeline
  incremental/               Live Dgraph sync pipeline
pkg/
  database/                  sqlx+pgx Postgres adapter behind an interface; goose migration helper (unused)
  dgraph/                    dgo/v250 adapter: round-robin, gzip, retry/backoff
  logger/                    slog helpers (app actually configures slog inline in main.go)
  util/                      Env parsing, string/date/struct helpers (well covered by tests)
  aws/, errors/, file/, httpx/, validator/   Support packages; aws/httpx/validator unused by ETL paths
scripts/                     One-shot EC2 host provisioning for Dgraph zero/alpha + swap file
deployment/
  dockerfiles/               Dockerfile-dev | -stg | -prd-1 | -prod
  pipelines/                 Azure Pipelines: build.yml + per-env stages + publish template
Dockerfile                   Root image (distroless, no .env baked in)
out/traceability-dgraph-etl  Committed ~16 MB build artifact
traceability-dgraph-etl      Committed ~15.7 MB build artifact (gitignored by name)
```

Note: both binaries listed above are present in the working tree. `.gitignore:40` lists `traceability-dgraph-etl` with no slash, so the pattern matches that basename at any depth — `git check-ignore` confirms **both** the root and `out/` copies are ignored.

---

## 3. Startup and wiring (`main.go`)

1. `signal.NotifyContext` for `SIGINT`/`SIGTERM` — cancellation propagates through every `errgroup` and channel in the ETL.
2. `godotenv.Load()` — missing `.env` only prints a notice.
3. `app.NewConfig()` — reads `config/app.yaml` relative to the **working directory**, then applies env overrides. **The app must be run from the repo root** (or a directory containing `config/app.yaml`).
4. `setupLogger` — `slog` JSON handler on stdout, with `msg` remapped to `message` (`pkg/logger.MessageKey`). Level from `LOGGER_LEVEL` (`debug|warn|error`, otherwise info). Note `main.go` builds this handler inline rather than calling `pkg/logger.SetupJsonFormatter()` / `logger.Init()`, which are both unused.
5. Postgres adapter from `cfg.DBClean` — **required in both modes**. `cfg.DBCleanReplica` is parsed from config but never connected or used.
6. Dgraph adapter — **only constructed when mode is `incremental`**.
7. Maps `app.*Config` → package-local config types (`database.Config`, `dgraph.Config`, `incremental.Config`) so `internal/etl/**` never imports `internal/app`.
8. `etl.Run(ctx, runnerCfg, deps)`. `OutputDir` is hardcoded to `./initial_imports`.

Failure path: any error returns up to `main`, prints `etl failed: ...` to stderr, exits 1.

---

## 4. Configuration

Two layers, YAML first then env, merged in `overrideWithEnv` (`internal/app/config.go`). Every field falls back to the YAML value when the env var is unset.

| Group | Env prefix | Notes |
|---|---|---|
| App | `APP_DEBUG`, `APP_MODE` | `APP_DEBUG` is parsed but never read by any code path |
| Logger | `LOGGER_LEVEL` | |
| Primary Postgres | `DB_CLEAN_*` | The only DB actually connected |
| Replica Postgres | `DB_CLEAN_REPLICA_*` | Parsed, never used |
| Dgraph | `DGRAPH_*` | `DGRAPH_ALPHAS` is comma-separated (`util.GetEnvAsStringSlice`); an empty string falls back to YAML |
| Initial | `INITIAL_WORKER_COUNT`, `INITIAL_CHUNK_SIZE` | **Absent from `config/app.yaml` and `.env.example`** — present in `config.go` and in the CI-generated `.env` only |
| Incremental | `INCREMENTAL_FETCH_CHUNK_SIZE`, `INCREMENTAL_{REMOVAL,EVENTS,LINKS}_*` | Per-phase batch sizes and worker counts |
| AWS S3 | `AWS_S3_*`, `AWS_ACCESS_KEY_ID`, … | Parsed; `pkg/aws` is not imported by any ETL code |

**Defaults resolve in two places.** YAML/env supply one set; `incremental.Config.ApplyDefaults()` replaces any non-positive value with its own fallback (lookup/delete/cleanup batch 100, concurrency 4, upsert batch **50**, `FetchChunkSize` 10000). Note `config/app.yaml` ships `upsert_batch_size: 10` while the code fallback is 50 — the YAML wins when present. `ApplyDefaults` is called twice (in `runner.go` and again in `NewMigrator`), which is idempotent.

Initial-mode defaults when unset: `workerCount = runtime.NumCPU()`, `chunkSize = common.DefaultChunkSize()` = 1,000,000 records.

**Adding a tunable touches up to 6 files** — `config/app.yaml`, `internal/app/config.go` (struct + `GetEnvAs*` line), `.env.example`, `deployment/pipelines/stages/build.yml` (the generated `.env`), and for incremental knobs also `internal/etl/incremental/config.go` + the `toIncrementalConfig` mapper in `main.go`.

---

## 5. Data model

### Postgres (schema not in this repo — inferred from queries)

| Table / view | Columns used | Role |
|---|---|---|
| `events_bc` | `unique_id` (key), `status` (`valid`/`invalid`), `sync_time`, plus ~40 attribute columns | Event source |
| `links_bc` | `id` (bigint key), `event1_id`, `event2_id`, `event1_version`, `event2_version`, `role`, `qty`, `unit`, `source`, `status`, `sync_time` | Directional edge source |
| `subjects` | `subject_id`, `subject_type`, `master_id`, `master_type` | Location/source enrichment |
| `mv_subject_hierarchy` | `start_subject_id`, `subject_id`, `subject_type`, `master_id`, `master_type` | Facility-level enrichment |
| `master_mill` | `uml_id`, `mill_id` | `uml_id → mill_id` lookup (de-duplicated via `GROUP BY`) |
| `traceability_log_daily` | `entry_date`, `status` | Upstream-ETL completion gate |
| `mv_events`, `monitoring_cache_shipments`, `mv_shipment_list` | — | Materialized views; refreshed by a separate Step Functions state, **not** by this ETL |

`sync_time IS NULL` is the **change-data-capture watermark** for both `events_bc` and `links_bc`.

### Dgraph — `schema.dql` (the deployed model)

One flat `Event` type. `unique_id: string @index(exact, hash, trigram) @upsert` is the business key; `material_code` also indexed (`hash, trigram`). `process_date` is `int`, `amount` is `float`, everything else `string`. The single edge predicate is `links: [uid] @reverse`.

`denorm_schema.dql` models the same domain normalised into `event.*` predicates with separate `Client`, `Plant`, `Subject`, `Unit`, `ProductionLabel`, `Flag`, `SubDataSource`, `Uml`, `Mill` types. **No Go code references it** — treat it as a design exploration, not the deployed schema.

**Neither schema file is loaded by the binary.** `dgraph.Adapter.SetSchema` exists but is never called; the Dockerfiles merely `COPY schema.dql` into the image. Adding a predicate in Go therefore also requires pushing the schema to the cluster by hand.

### Edge facets

Both modes attach the same six facets to `<links>`, built only for non-empty values (`buildLinkFacets` in `incremental/links.go`, inline equivalent in `initial/links.go`): `event1_version`, `event2_version`, `role`, `qty` (bare float, unquoted), `unit`, `source`.

---

## 6. Shared RDF machinery (`internal/etl/common`)

- **`BlankNode(prefix, value)`** — deterministic blank-node IDs. Purely alphanumeric values pass through as `_:event_<value>`; anything else is sanitised to alphanumerics (non-alphanumerics → `_`, `__` collapsed, trimmed) **and** suffixed with the hex SHA-1 of the original, so `"A-1"` and `"A_1"` cannot collide. Empty value → `_:<prefix>_unknown`. Covered by `uid_test.go`. Changing this function invalidates any already-loaded graph and breaks the events↔links join in initial mode.
- **`StringInterner`** — 32 shards, FNV-1a dispatch, `sync.RWMutex` per shard, double-checked write. Collapses repeated column values across millions of rows. One instance per migrator, shared by all workers.
- **`ChunkedGzipRDFWriter`** — `Write` pushes a string onto a buffered channel (cap 1024); a single background goroutine owns the file, so concurrent writers never contend on I/O. Layering is `os.File → gzip.Writer → bufio.Writer (1 MiB)`. Rolls over every `chunkSize` **Write calls** (one call = one event's whole triple block, i.e. one source row) to `base_1.rdf.gz`, `base_2.rdf.gz`, …, keeping the un-suffixed name for chunk 0. Once an error is recorded the run loop drains the channel so producers never deadlock; the error surfaces from `Close()`.
- **`RDFWriter`** — the uncompressed, non-chunked equivalent. **Dead code**: nothing constructs it.
- **Literal helpers** — `StringLiteral` (via `strconv.Quote`), `IntLiteral`/`FloatLiteral` with XSD type annotations, `AppendTriple`.

---

## 7. Mode 1 — `initial` (RDF export)

**Pipeline** (`initial/migrator.go`): `os.RemoveAll(outputDir)` → `MkdirAll` → one fetcher goroutine streams `eventsQuery` into a channel of capacity `workerCount*4` → `workerCount` aggregator workers build and write RDF → close event writers → **then** stream links single-threaded. All under one `errgroup`, so the first error cancels everything.

**Destructive**: `initial_imports/` is deleted wholesale at the start of every run.

**Events query** (`initial/events.go`) selects events that participate in at least one valid link: a `UnsyncedLinks` CTE inner-joins `links_bc` to two `events_bc` rows requiring `status='valid'` on the link and both endpoints, unions `event1_id`/`event2_id` into `UnsyncedUniqueIDs`, then joins `events_bc` plus `subjects` ×2, `mv_subject_hierarchy` ×2, and `MasterMill`. Despite the CTE name, **no `sync_time` filter is applied in initial mode** — it is a full export of link-connected valid events.

**Per-event RDF** (`initial/aggregator.go`): `<dgraph.type> "Event"`, `<unique_id>`, then one triple per non-empty attribute. Empty/NULL values are simply omitted. Subject is `BlankNode("event", unique_id)`. Builders come from a `sync.Pool`.

**Links query** (`initial/links.go`): all `links_bc` rows where the link and both endpoint events are `status='valid'`. Each row emits one `_:event_<e1> <links> _:event_<e2> (facets) .` into `links.rdf.gz`.

**Output**: `initial_imports/events.rdf.gz` (+ `events_N.rdf.gz`) and `initial_imports/links.rdf.gz` (+ `links_N.rdf.gz`). The two files join purely by blank-node identity, which is why `BlankNode` must stay stable within a run.

**Normalisation asymmetry** (`initial/normalize.go`): `String()` only trims; `InternedString()` trims **and** interns. The aggregator interns only a subset of columns (low-cardinality ones like `product_id`); high-cardinality columns like `unique_id` are deliberately not interned. The incremental normalizer, by contrast, interns everything.

---

## 8. Mode 2 — `incremental` (live sync)

`Migrator.Run` executes a fixed sequence, one file per step:

### 8.0 Gate — `traceability_check.go`
Computes `now(Asia/Jakarta) - 2 days` and polls `traceability_log_daily` for any row with `entry_date + 1 day = target AND status = 'Running'`. While running, it sleeps **15 minutes** and re-checks, indefinitely (only `ctx` cancellation exits). `sql.ErrNoRows` is treated as Done with a warning. This blocks the job rather than racing the upstream traceability ETL. `time.LoadLocation("Asia/Jakarta")` requires tzdata — relevant for the distroless images.

### 8.1 Removal — `removal.go`
Query: link IDs and event `unique_id`s where `sync_time IS NULL AND status = 'invalid'` (unioned into one two-column result set). Resolves `unique_id → uid` against Dgraph, then issues `<uid> * * .` whole-node deletes. Returns both ID sets for the later sync-time stamping — **link IDs are collected even though nothing is deleted from the graph for them**, so their `sync_time` still gets stamped.

### 8.2 Events — `events.go`
`eventIDsQuery` collects `unique_id`s from two sources, unioned: endpoints of links with `sync_time IS NULL` whose event is `valid`, plus events with `sync_time IS NULL AND status='valid'` that participate in at least one valid link.

Processed in chunks of `FetchChunkSize`; per chunk: `lookupEventUIDs` → merge into the shared `uidMap` → `fetchEventDetails` (`WHERE unique_id = ANY($1) AND status='valid'`, with the same `subjects`/`mv_subject_hierarchy`/`master_mill` enrichment) → `upsertEvents`.

**Upsert is set-or-delete, field by field.** A present value emits a `set` triple; an empty one emits `<s> <p> *` into a parallel delete builder, and both go out in one `MutateWithDeletesRDF` call — so a value cleared in Postgres is actually cleared in Dgraph. A `unique_id` already in the `uidMap` is addressed as `<0x...>`; a miss becomes a `BlankNode`, and the uids Dgraph returns are mapped back through `blankToID` and merged into the `uidMap` so later phases can reference the new nodes.

### 8.3 Links — `links.go`
`linkEvent1IDsQuery` resolves a **set of `event1_id`s** to re-process: endpoints of unsynced links, all events with `sync_time IS NULL`, plus `event1_id` of any link whose `event2_id` is an unsynced event (so an edge is rewritten when only its target changed).

Chunking is **by `event1_id`, never by link row** — `fetchLinkDetails` pulls *every* link for those event1s (`WHERE event1_id = ANY($1)`, including invalid ones, joining `events_bc` for `event2.status`), and `deleteExistingLinks` wipes `<event1_uid> <links> *` for the whole chunk before `upsertLinks` writes the valid ones back. Splitting one event1's links across two chunks would make the second chunk erase the first chunk's writes; the code carries this as an explicit comment.

Within a chunk: all link IDs are recorded for sync-time stamping regardless of validity; only links where both `link.status` and `event2.status` are `valid` become records; event IDs absent from the `uidMap` are collected into `missingUIDs` and looked up before the delete/upsert.

Finally `cleanupMissingLinks` strips `<links>` from every synced event that turned out not to be `event1` of any link.

### 8.4 Sync time — `sync_time.go`
`UPDATE events_bc SET sync_time = NOW() WHERE unique_id IN (...) AND sync_time IS NULL`, and the equivalent for `links_bc` by `id`, both in hardcoded batches of **200** parameters. The ID set is the union of removal + event sync + link sync results. Errors from the two updates are joined, not short-circuited.

**This is the restartability mechanism**: stamping happens only after the graph mutations succeed, so a crashed run leaves `sync_time` NULL and the next run re-processes the same rows. Never stamp earlier.

### 8.5 Materialized views — out of scope
The incremental ETL does not refresh materialized views. `mv_events`, `monitoring_cache_shipments` and `mv_shipment_list` are refreshed by a separate Step Functions state downstream of this task (`materialized_views.go` was removed).

### Incremental concurrency model
`fanOutBatches[T]` (`helpers.go`) is the **only** concurrency primitive: a producer goroutine feeds an unbuffered job channel consumed by N workers, all under an `errgroup`. Every batch size and worker count it receives comes from config. The shared `uidMap` is an RWMutex-guarded map with `Get`/`Set`/`Merge`/`Values`. `stringSet` is a plain `map[string]struct{}` that silently drops empty strings and is **not** goroutine-safe — it is only used single-threaded.

---

## 9. Code vs. `command.txt` — verified drift

The brief is older than the code. Differences found:

- The brief joins `subject_facility_map_mv`; **the code uses `mv_subject_hierarchy`**.
- The code selects columns absent from the brief's mapping: `material_document`, `purchase_order`, `vendor`, `movement_type`, `movement_type_text`, `storage_location`, `partner_pca`, `mill_name`. All exist in `schema.dql`.
- The brief's initial events query uses `EXISTS` subqueries; the code uses inner joins.
- The brief's step 4 for incremental links ("set event.links empty") is implemented as `cleanupMissingLinks` plus the per-chunk `deleteExistingLinks`.

Treat the **code** as current and the brief as intent.

---

## 10. Infrastructure packages (`pkg/`)

Interface-first: each package has a `contract.go` declaring the `Adapter` interface and a sibling file implementing it, so ETL code depends on `database.Adapter` / `dgraph.Adapter`, never on concrete clients.

**`pkg/database`** — `sqlx` over the `pgx/v5` stdlib driver. The DSN is assembled by `fmt.Sprintf` with **`sslmode=disable` hardcoded** and `search_path`, `connect_timeout`, `client_encoding` from config. Pool limits from `MaxConnectionOpen`/`MaxConnectionIdle`/`MaxLifeTimeMS`. `NewConnectionPostgres` pings eagerly and fails fast. `database_replica.go` implements the read-only `AdapterReplica`; `migration.go` wraps goose — neither is wired into `main.go`.

**`pkg/dgraph`** — wraps `dgo/v250`. One alpha → `dgo.NewClient`; several → `dgo.NewRoundRobinClient`. gRPC gzip compression is always on. **TLS is not implemented**: `SSLModeRequire` and `SSLModeVerifyCA` both fall through to `insecure.NewCredentials()` with a placeholder comment, so `DGRAPH_SSL_MODE` currently has no effect.

Retry policy differs by operation:
- Queries (`RunDQL`, `RunDQLWithVars`): default 3 retries, backoff `100ms × 2^(n-1)` capped at 2s.
- Mutations (`MutateRDF`, `MutateWithDeletesRDF`, `DeleteRDF`): default 7 retries, backoff `200ms × 2^(n-1)` + 0–100 ms jitter, capped at 5s, each attempt wrapped in its own transaction timeout (`TransactionTimeoutSecond`, else 45 s, else `TimeoutSecond` if smaller). `CommitNow: true` — every mutation is its own committed transaction, so a failed batch mid-run leaves earlier batches applied (safe because `sync_time` has not been stamped).

`isRetryableError` checks gRPC codes `Aborted`/`Unavailable`/`DeadlineExceeded`/`ResourceExhausted`, then falls back to substring matching on ~18 patterns including `"conflict"`, `"timeout"`, `"eof"`, and **`"context canceled"`** — note that this makes a cancelled context look retryable to the string path.

Lookups use `graph.WithBestEffort()` + `graph.WithReadOnly()` and parse `{ nodes(func: type(Event)) @filter(eq(unique_id, "...", ...)) { uid unique_id } }` into a `unique_id → uid` map; JSON unmarshal failures return an empty map rather than an error.

**`pkg/util`** — env parsing (`GetEnv`, `GetEnvAsInt`, `GetEnvAsBool`, `GetEnvAsStringSlice`), plus string/date/struct/random/password helpers. This is where the repo's test coverage lives.

**Unused by any ETL path**: `pkg/aws` (S3), `pkg/httpx`, `pkg/validator`, `pkg/errors`, most of `pkg/logger`, `internal/app/consts/{api,file,http_header}.go`.

---

## 11. Dependencies

Go **1.25.3** (`go.mod`; root `Dockerfile` pins the same, the deployment Dockerfiles use `golang:1.25.4-alpine`).

Direct: `dgraph-io/dgo/v250`, `jackc/pgx/v5` + `jmoiron/sqlx`, `joho/godotenv`, `gopkg.in/yaml.v3`, `golang.org/x/sync` (errgroup), `google.golang.org/grpc`, `go-playground/validator/v10`, `pressly/goose/v3`, `aws-sdk-go-v2` (+ config/credentials/s3), `golang.org/x/crypto`, `stretchr/testify`.

Logging is stdlib `log/slog` — no third-party logger.

---

## 12. Build, run, test

The Go toolchain is **not on `PATH` in the current Windows dev environment** (verified: `go` not found in both bash and PowerShell), so the commands below were not executed here.

```bash
go build -o traceability-dgraph-etl ./main.go
go run ./main.go                                  # mode from APP_MODE / config/app.yaml
go test ./...                                     # tests live in pkg/util and internal/etl/common
go test ./pkg/util/ -run TestParseDate -v         # single test
go vet ./...

docker build -t traceability-dgraph-etl .
```

Run from the repo root so `config/app.yaml` resolves. There is **no Makefile, no linter config, no CI test or lint step** — `deployment/pipelines/` only builds and pushes images.

Tests present: `internal/etl/common/uid_test.go` and ~12 files under `pkg/util/`. **No tests exist for `internal/etl/initial` or `internal/etl/incremental`** — the ETL logic itself is untested, and both packages require live Postgres/Dgraph to exercise.

Cluster provisioning (not part of the app lifecycle): `scripts/setup_dgraph_zero.sh`, `scripts/setup_dgraph_alpha.sh <ZERO_IP>`, `scripts/swap.sh`. These tune file descriptors, sysctl, and systemd units on EC2 hosts and contain **hardcoded private IPs** (`172.30.0.65`, `172.30.0.95`, `172.30.0.171`, `172.30.0.36`), a hardcoded `raft idx=3`, and a trailing `rsync` line in the alpha script that is outside any function and would run on execution.

---

## 13. Deployment

`deployment/pipelines/azure-pipelines.yml` triggers on branches `gar-dev`, `gar-stg`, `gar-prd-1`.

1. **Build stage** — selects the Azure variable group (`trace-dgraph-etl-{dev,stg,prd-1}`) by branch, writes a `.env` file from those variables into the source directory, and publishes the whole tree as the `DockerBuildContext` artifact.
2. **Per-env stage** — guarded by `eq(variables['Build.SourceBranch'], ...)`, downloads the artifact, builds `deployment/dockerfiles/Dockerfile-<env>` via `publish-template.yml` (`docker build --no-cache`), and pushes to ECR tagged with `$(Build.BuildId)`.

Image differences worth knowing:
- The **root `Dockerfile`** uses `distroless/base-debian12:nonroot`, copies `config/` and `schema.dql`, and does **not** bake a `.env`.
- The **deployment Dockerfiles** use `distroless/static-debian12:nonroot` and **do** `COPY .env` — they only build if the Build stage generated that file. They also `EXPOSE 8080` despite the app serving nothing, and name the binary `traceability-dgrapg-etl` (transposed letters — consistent within those files, so it works).
- `Dockerfile-prod` and `Dockerfile-prd-1` are byte-identical; only `-prd-1` is referenced by a stage.

The generated `.env` in `build.yml` omits `DB_CLEAN_SCHEMA`-adjacent entries like `DB_CLEAN_MAX_LIFE_TIME_MS` / `DB_CLEAN_TIMEOUT_SECOND` / `DB_CLEAN_CHARSET`, `DGRAPH_TRANSACTION_TIMEOUT_SECOND`, and every `INCREMENTAL_{REMOVAL,EVENTS,LINKS}_*` knob — those fall back to `config/app.yaml` baked into the image. Adding an env var without updating `build.yml` means it silently keeps the YAML value in deployed environments.

Scheduling is not defined in this repo: nothing here runs the container daily. The "daily" cadence of incremental mode is external (verified absent, not merely unread).

---

## 14. Conventions

- `.editorconfig`: LF, UTF-8, final newline, trimmed trailing whitespace; Go uses tabs with width 2 and a 120-column guide; YAML/JSON/DQL/SQL/shell use 2-space indent.
- Errors are wrapped with `fmt.Errorf("context: %w", err)` at every layer; multi-error paths use `errors.Join`.
- Logging is structured `slog` with snake_case keys (`slog.Int("chunk_index", i)`).
- Config structs carry both `yaml` and `json` tags.
- Package-private helpers are lower-case; only cross-package API is exported — `internal/etl/initial` and `internal/etl/incremental` export almost nothing beyond their `Migrator`.

---

## 15. Knowledge gaps — not verifiable from this repository

1. **Git history is empty.** The branch is `main` with **zero commits**; everything is staged/untracked. No commit conventions, authorship, or change rationale can be derived. The CI's `gar-*` trigger branches do not exist locally.
2. **No Postgres DDL.** Table and view definitions for `events_bc`, `links_bc`, `subjects`, `mv_subject_hierarchy`, `master_mill`, `traceability_log_daily`, `mv_events`, `monitoring_cache_shipments`, `mv_shipment_list` live elsewhere. Column types above are inferred from `sql.Null*` scan targets and query usage. Index coverage — critical for the `= ANY($1)` and `sync_time IS NULL` scans — is unknown.
3. **Who populates `sync_time`/`status`.** The upstream traceability ETL that writes `traceability_log_daily` and flips `status` is a separate system.
4. **Bulk-loader invocation.** How `initial_imports/*.rdf.gz` reaches Dgraph (`dgraph bulk` flags, `--schema`, output `p` directory handling) is not in this repo; `setup_dgraph_alpha.sh` only hints at an rsync of `/data/alpha/p/`.
5. **How `schema.dql` is applied** to a live cluster, and whether the deployed schema currently matches the file.
6. **Whether `denorm_schema.dql` is aspirational or abandoned**, and whether any consumer queries the denormalised predicates.
7. **Runtime scheduling and resource sizing** for the ETL container (cron/EventBridge/K8s CronJob), and real-world values for worker counts, chunk sizes, and run duration.
8. **Correct values for the hardcoded IPs** in `scripts/`, and which environment they describe.
9. **The owner's external flow documentation** referenced in `command.txt` is not in the repo.
10. **Build/test verification**: no Go toolchain in this environment, so nothing here has been compiled or run — all behaviour above is read from source, not observed.
