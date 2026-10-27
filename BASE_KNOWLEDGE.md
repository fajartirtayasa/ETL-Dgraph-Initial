# BASE_KNOWLEDGE.md

Reference knowledge for `ETL-Dgraph-Initial` (Go module `hara/traceability-dgraph-etl`), derived from reading the
source, config, schemas and tests. Updated 2026-10-28: the repository holds only the initial export, and the export
now stamps `sync_time`.

`CLAUDE.md` holds the short operational guidance (commands, invariants to respect when editing). This document is the
deeper reference: exact queries and statements, field mappings, module responsibilities, and verified gaps. Where the
two overlap, `CLAUDE.md` is the summary and this is the detail.

> **History.** Until commit `b487294` this repository also held an `incremental` mode (live Dgraph sync), `pkg/dgraph`
> and an ECS / Azure Pipelines deployment. All three were removed. The incremental ETL is now the Python repository
> `ETL-Dgraph-Incremental`. The earlier version of this document, which describes that code, is
> `git show b487294:BASE_KNOWLEDGE.md`. Decisions are recorded in the Dgraph repository's `REFACTOR_PLAN.md` (§14.1,
> M14–M38).

---

## 1. Project overview

A single-binary batch ETL that exports **traceability data** from **PostgreSQL**: supply-chain `events` and the
directional `links` between them, at a stated scale of millions of rows (`command.txt`). The output is gzipped,
chunked RDF N-Quad files in `./initial_imports/`, for the Dgraph bulk loader (`dgraph bulk`) to ingest **out of band**.
The binary never connects to Dgraph.

After the RDF is complete, the binary **stamps `sync_time = NOW()`** on the source rows it exported (§8), so the Python
incremental ETL, which selects on `sync_time IS NULL`, starts from the exported baseline. That stamp is the binary's
only write to PostgreSQL, and `INITIAL_STAMP_SYNC_TIME=false` turns it off.

There is one mode. `APP_MODE` must be `initial` (trimmed, any case). Anything else, including an empty value, is
rejected at startup, before PostgreSQL is contacted.

Three sibling repositories work together:

| Folder | Role |
|---|---|
| `ETL-Dgraph-Initial` | this repository: the Go export and the baseline stamp |
| `ETL-Dgraph-Incremental` | the Python incremental ETL; owns the canonical `schema.dql` and the bulk-load runbook (`deploy/staging/INV7_BULK_LOAD.md`); reads this repository's synthetic fixture in place |
| `Dgraph` | the live `REFACTOR_PLAN.md` and `REFACTOR_ASSESSMENT.md` |

**`command.txt`** is the owner's original requirements brief for both strategies (initial and incremental),
including field mappings and the constraint *"don't create any documentation flow technical (!important, because i
already have)"*. In other words, flow documentation is maintained outside this repository. It is kept unchanged
because the Python repository relies on it for field mappings. Its SQL has drifted from the code (§9).

---

## 2. Directory structure

```
main.go                      Composition root: config → mode and stamping checks → logger → Postgres → etl.Run
config/app.yaml              YAML defaults
.env.example                 Env var names (every one overrides the YAML)
build.sh                     Builds out/traceability-dgraph-etl (linux/arm64, static) + out/config/app.yaml
schema.dql                   Dgraph schema, byte-identical mirror of ../ETL-Dgraph-Incremental/schema.dql — applied MANUALLY
denorm_schema.dql            Alternative normalised schema — referenced by NO code
command.txt                  Owner's requirements brief (kept unchanged)

internal/app/                Config struct + env override layer
  consts/                    ConfigPath="config/", ConfigFile="app.yaml", plus unused api/file/http_header consts
internal/etl/
  mode.go                    ParseMode: only "initial"
  stamp.go                   ParseStampSyncTime: INITIAL_STAMP_SYNC_TIME
  runner.go                  Rejects any other mode, builds the migrator
  common/                    Shared RDF machinery (blank nodes, interner, writers, literals)
  initial/                   The export pipeline and the sync_time stamp
pkg/
  database/                  sqlx+pgx Postgres adapter behind an interface; goose migration helper (unused)
  logger/                    slog helpers (main.go configures slog inline and uses only MessageKey)
  util/                      Env parsing, string/date/struct helpers (well covered by tests)
  aws/, errors/, file/, httpx/, validator/   Support packages; only file/ (YAML loading) is used
parity/
  go.sh                      Runs go locally or in the pinned container, with both Go/Python repositories mounted
  fixture/                   FROZEN synthetic Postgres fixture shared with the Python repository
  guards/                    Synthetic-data, credential and frozen-fixture guards
setup/                       Historical Dgraph host-provisioning scripts; unmaintained, not part of the app
```

`out/` and `initial_imports/` are gitignored. Nothing builds a container image: there is no Dockerfile and no CI.

---

## 3. Startup and wiring (`main.go`)

1. `signal.NotifyContext` for `SIGINT`/`SIGTERM`. Cancellation propagates through the export's `errgroup`, its
   channels and the stamping statements (`ExecContext`).
2. `godotenv.Load()`. A missing `.env` only prints a notice.
3. `app.NewConfig()` reads `config/app.yaml` relative to the **working directory**, then applies the env overrides.
   Run the binary from the repository root, or from a directory holding `config/` (which is what `out/` is).
4. `etl.ParseMode(cfg.App.Mode)` and `etl.ParseStampSyncTime(cfg.Initial.StampSyncTime)`. Either error ends the run
   **before** any connection.
5. `setupLogger` sets up an `slog` JSON handler on stdout, with the message key renamed to `message`
   (`pkg/logger.MessageKey`). The level comes from `LOGGER_LEVEL` (`debug|warn|error`, otherwise info).
6. The Postgres adapter is built from `cfg.DBClean`. `cfg.DBCleanReplica` is parsed but never connected.
7. `app.*Config` is mapped to package-local types (`database.Config`, `etl.RunnerConfig`), so `internal/etl/**` never
   imports `internal/app`.
8. `etl.Run(ctx, runnerCfg, deps)`. `OutputDir` is hardcoded to `./initial_imports`.

Failure path: any error returns up to `main`, which prints `etl failed: ...` to stderr and exits 1. That includes a
stamping failure after a complete export (§8).

---

## 4. Configuration

Two layers, YAML first and then env, merged in `overrideWithEnv` (`internal/app/config.go`). Each field keeps its YAML
value when its env var is unset.

| Group | Env vars | Notes |
|---|---|---|
| App | `APP_DEBUG`, `APP_MODE` | `APP_DEBUG` is parsed but never read. `APP_MODE` is strict (only `initial`); a blank `APP_MODE=` line **overrides** the YAML with an empty value and fails, which is why `.env.example` ships `APP_MODE=initial` |
| Logger | `LOGGER_LEVEL` | |
| Primary Postgres | `DB_CLEAN_*` | The only database connected; it is read by the export and written by the stamp |
| Replica Postgres | `DB_CLEAN_REPLICA_*` | Parsed, never used |
| Initial | `INITIAL_WORKER_COUNT`, `INITIAL_CHUNK_SIZE`, `INITIAL_STAMP_SYNC_TIME` | Blank means the default. Worker count: `runtime.NumCPU()`. Chunk size: 1,000,000 records. Stamping: `true` |
| AWS S3 | `AWS_S3_*`, `AWS_ACCESS_KEY_ID`, … | Parsed; `pkg/aws` is not imported by the ETL |

**`INITIAL_STAMP_SYNC_TIME`** is kept as text (`initial.stamp_sync_time: "true"` in the YAML), not a `bool`, for two
reasons. A YAML without the key would otherwise mean `false`, and `util.GetEnvAsBool` silently falls back to the
default on a value it cannot parse. `etl.ParseStampSyncTime` treats a blank value as `true` and accepts what
`strconv.ParseBool` accepts (trimmed, any case). Anything else, such as `no` or `off`, is a startup error.

**Adding a tunable touches three files**, which must stay in sync: `config/app.yaml` (default + inline comment),
`internal/app/config.go` (struct field + `GetEnv*` line), and `.env.example`.

---

## 5. Data model

### PostgreSQL (no DDL in this repository — inferred from queries and the fixture)

| Table / view | Columns used | Role |
|---|---|---|
| `events_bc` | `unique_id` (business key; the owner confirms one **valid** row per `unique_id`, the DDL does not enforce it), `status` (`valid`/`invalid`), `sync_time`, plus ~40 attribute columns | Event source |
| `links_bc` | `id` (bigint key), `event1_id`, `event2_id`, `event1_version`, `event2_version`, `role`, `qty`, `unit`, `source`, `status`, `sync_time` | Directional edge source |
| `subjects` | `subject_id`, `subject_type`, `master_id`, `master_type` | Location/source enrichment |
| `mv_subject_hierarchy` | `start_subject_id`, `subject_id`, `subject_type`, `master_id`, `master_type` | Facility-level enrichment (a materialized view) |
| `master_mill` | `uml_id`, `mill_id` | `uml_id → mill_id` lookup (de-duplicated via `GROUP BY`) |

**`sync_time IS NULL` is the change-data-capture watermark** for `events_bc` and `links_bc`. The Python incremental
ETL selects on it and stamps rows after its graph writes succeed. This binary's export **ignores** `sync_time`, and its
stamp (§8) sets it on the exported baseline. Setting it back to `NULL` when a source row changes happens outside both
ETLs (`REFACTOR_PLAN.md` PD-20).

### Dgraph — `schema.dql`

One flat `Event` type. `unique_id: string @index(exact, hash, trigram) @upsert` is the business key; `material_code`
is also indexed (`hash, trigram`). `process_date` is `int`, `amount` is `float`, and everything else is `string`. The
single edge predicate is `links: [uid] @reverse`.

`schema.dql` **must stay byte-identical** to `../ETL-Dgraph-Incremental/schema.dql`, which is canonical and the copy the
bulk-load runbook uses. Two tests enforce this (§12). The binary never loads a schema; it is applied by hand
(`dgraph bulk --schema`).

`denorm_schema.dql` models the same domain normalised into `event.*` predicates with separate `Client`, `Plant`,
`Subject`, `Unit`, `ProductionLabel`, `Flag`, `SubDataSource`, `Uml`, `Mill` types. **No code references it** — treat
it as a design exploration.

### Edge facets

Each `<links>` edge carries up to six facets, emitted only for non-empty values (`buildLinkFacets`, `initial/links.go`):
`event1_version`, `event2_version`, `role`, `qty` (bare float, unquoted), `unit`, `source`.

---

## 6. Shared RDF machinery (`internal/etl/common`)

- **`BlankNode(prefix, value)`** returns deterministic blank-node IDs.
  - A purely alphanumeric value passes through as `_:event_<value>`.
  - Anything else is sanitised to alphanumerics (non-alphanumerics → `_`, `__` collapsed, trimmed) **and** suffixed
    with the hex SHA-1 of the original, so `"A-1"` and `"A_1"` cannot collide.
  - An empty value becomes `_:<prefix>_unknown`.
  - Covered by `uid_test.go`. The events and links files are joined only by these IDs, so changing the function breaks
    that join.
- **`StringInterner`**: 32 shards with FNV-1a dispatch, a `sync.RWMutex` per shard and a double-checked write.
  Collapses repeated column values across millions of rows. One instance per migrator, shared by all workers.
- **`ChunkedGzipRDFWriter`**:
  - `Write` pushes a string onto a buffered channel (capacity 1024), and a single background goroutine owns the file.
    Layering is `os.File → gzip.Writer → bufio.Writer (1 MiB)`.
  - It rolls over every `chunkSize` **Write calls** (one call = one source row's block of triples) to `base_1.rdf.gz`,
    `base_2.rdf.gz`, …; chunk 0 keeps the un-suffixed name.
  - `Write` returning nil only means the line was queued. Write errors surface from `Close()`, which waits for the
    goroutine to flush and close the file. After an error the goroutine keeps draining the channel, so producers never
    deadlock.
- **`RDFWriter`**: the uncompressed, non-chunked equivalent. **Dead code**: nothing constructs it.
- **Literal helpers**: `StringLiteral` (via `strconv.Quote`), `IntLiteral`/`FloatLiteral` with XSD type annotations,
  and `AppendTriple`.

---

## 7. The export (`internal/etl/initial`)

**Pipeline** (`migrator.go`, `Run`):
1. `os.RemoveAll(outputDir)`, then `MkdirAll`. **Destructive**: `initial_imports/` is deleted wholesale at the start
   of every run.
2. One fetcher goroutine streams `eventsQuery` into a channel of capacity `workerCount*4`.
3. `workerCount` aggregator workers build and write the RDF, all under one `errgroup`, so the first error cancels
   everything.
4. The event writer is closed, **then** the links are streamed single-threaded into `links.rdf.gz` (`streamLinks`).
5. Once both writers have closed without error, the `sync_time` stamp runs (§8) unless it is disabled.

**Shared filters** (`events.go`). The export queries are built from two constants, which the stamping statements reuse:
- **`exportedLinksFilter`** selects every exported link: a link with `status = 'valid'` whose `event1_id` and
  `event2_id` both match an `events_bc` row with `status = 'valid'`.
  ```sql
  FROM links_bc l
  JOIN events_bc e1 ON l.event1_id = e1.unique_id AND e1.status = 'valid'
  JOIN events_bc e2 ON l.event2_id = e2.unique_id AND e2.status = 'valid'
  WHERE l.status = 'valid'
  ```
- **`exportedEventIDsCTE`** builds `UnsyncedLinks` (the event IDs of those links) and `UnsyncedUniqueIDs` (the `UNION`
  of their `event1_id` and `event2_id`). Despite the names, nothing filters on `sync_time`.

**Events query.** `eventsQuery` = `exportedEventIDsCTE` + `MasterMill`, then `events_bc e JOIN UnsyncedUniqueIDs`, with
`LEFT JOIN`s to `subjects` ×2, `mv_subject_hierarchy` ×2 and `MasterMill`. It is a full export of link-connected
events. Two behaviours, read from the SQL:
- **No `status` filter on `e`.** An invalid `events_bc` row that shares its `unique_id` with an exported valid row is
  exported too, into the same blank node.
- **The `LEFT JOIN`s can produce more than one row per event** (several hierarchy rows, or several `mill_id`s per
  `uml_id`). Each row is written, so an event's triples can repeat.

**Per-event RDF** (`aggregator.go`): `<dgraph.type> "Event"`, `<unique_id>`, then one triple per non-empty attribute;
empty or NULL values are omitted. A row whose `unique_id` is blank after `strings.TrimSpace` is skipped. The subject is
`BlankNode("event", unique_id)`. Builders come from a `sync.Pool`.

**Links query.** `linksQuery` = `SELECT l.id, l.event1_id, …` + `exportedLinksFilter`. Each row emits one
`_:event_<e1> <links> _:event_<e2> (facets) .` into `links.rdf.gz`, unless `event1_id` or `event2_id` is blank after
trimming.

**Output**: `initial_imports/events.rdf.gz` (+ `events_N.rdf.gz`) and `initial_imports/links.rdf.gz`
(+ `links_N.rdf.gz`).

**Normalisation** (`normalize.go`): `String()` only trims; `InternedString()` trims **and** interns. Only
low-cardinality columns are interned (`product_id`, `unit`, subject types, …), not `unique_id`.

---

## 8. Stamping `sync_time` (`internal/etl/initial/sync_time.go`)

**Why.** Without a baseline stamp every row stays `NULL`, and the first incremental run would select the whole
dataset.

**When.** In `Run`, only after `streamLinks` returns nil, when every RDF file is complete on disk. The bulk load is
manual and comes **after** the stamp. A failed or cancelled export stamps nothing.

**What.** One `UPDATE` per table, `events_bc` first, each committed on its own. `links_bc` is attempted even if
`events_bc` fails, and both errors are returned (`errors.Join`). Each statement stamps only rows that were
**exported**, have `status = 'valid'` and have `sync_time IS NULL`. Links are stamped by `links_bc.id`. The log reports
the rows stamped per table (`"sync_time stamped"`, `table`, `rows`).

```sql
-- stampEventsStatement = exportedEventIDsCTE +
UPDATE events_bc e SET sync_time = NOW()
WHERE e.unique_id IN (SELECT unique_id FROM UnsyncedUniqueIDs)
  AND btrim(e.unique_id, $1) <> ''
  AND e.status = 'valid'
  AND e.sync_time IS NULL;

-- stampLinksStatement
UPDATE links_bc SET sync_time = NOW()
WHERE id IN (
    SELECT l.id <exportedLinksFilter>
      AND btrim(l.event1_id, $1) <> ''
      AND btrim(l.event2_id, $1) <> ''
)
  AND status = 'valid'
  AND sync_time IS NULL;
```

**How the exported rows are found.** The statements re-run the export's own filters, so **no ID is held in memory**.
`Exec` receives one argument, `$1` = `trimSpaceChars`: the characters `strings.TrimSpace` removes, tested equal to
`unicode.IsSpace` over every rune. That way rows the export skipped for a blank ID are skipped here too.

**Precondition (owner-guaranteed, not enforced in code).** `events_bc` and `links_bc` do not change from the start of
the export to the end of the stamp. Re-selecting is exact only under that guarantee.

**Rows left `NULL`** are for the incremental ETL:
- invalid rows, including invalid rows that share a `unique_id` with an exported valid row;
- links that are not valid at both ends;
- valid events without a fully valid link;
- rows with a blank ID.

Already-stamped rows are never touched, so a rerun stamps nothing new.

**Failure.** The RDF files are kept, and `Run` returns:
*"the RDF export in initial_imports is complete and kept, but stamping sync_time failed; re-run the stamping
statements (Dgraph REFACTOR_PLAN.md §9.0.1) before events_bc or links_bc change: stamp events_bc sync_time: …"*. The
binary exits 1. Nothing re-exports automatically.

To recover, run the statements by hand while the freeze still holds. The `psql` form is in `REFACTOR_PLAN.md` §9.0.1.
A failed statement rolled back as a whole, and the statements are idempotent.

**Off switch.** `INITIAL_STAMP_SYNC_TIME=false` writes the RDF and changes nothing in PostgreSQL. Because the export
ignores `sync_time`, a later run with stamping on exports everything again and stamps what is still `NULL`.

**Cost at scale.** Go memory stays flat. The work happens on the server:
- the CTE, the `UNION` and the `IN` subquery are bounded by `work_mem` and spill to disk;
- each table is one transaction, which produces a burst of WAL and holds row locks until commit;
- every stamped row leaves a dead tuple until vacuum.

These costs have not been measured beyond the synthetic fixture.

---

## 9. Code vs. `command.txt` — verified drift (initial strategy)

- The brief joins `subject_facility_map_mv`; **the code uses `mv_subject_hierarchy`**.
- The code selects columns absent from the brief's mapping: `material_document`, `purchase_order`, `vendor`,
  `movement_type`, `movement_type_text`, `storage_location`, `partner_pca`, `mill_name`. All are declared in
  `schema.dql`.
- The brief's events query uses `EXISTS` subqueries; the code uses inner joins.
- The brief's scope stops at the RDF files. The code also stamps `sync_time` on the exported rows (§8), an owner
  decision of 2026-10-28.

Treat the **code** as current and the brief as intent. Its incremental half describes what is now the Python ETL.

---

## 10. Infrastructure packages (`pkg/`)

**Interface-first.** Each package has a `contract.go` declaring its `Adapter` interface and a sibling file implementing
it, so the ETL depends on `database.Adapter`, never on a concrete client.

**`pkg/database`** is `sqlx` over the `pgx/v5` stdlib driver.
- The DSN is assembled with `fmt.Sprintf`, with **`sslmode=disable` hardcoded**, and `search_path`, `connect_timeout`
  and `client_encoding` from config.
- Pool limits come from `MaxConnectionOpen` / `MaxConnectionIdle` / `MaxLifeTimeMS`.
- `NewConnectionPostgres` pings eagerly and fails fast.
- The ETL uses `QueryRows` (both export queries, as streaming cursors) and `Exec` (the two stamping statements only).
- `database_replica.go` (read-only `AdapterReplica`) and `migration.go` (goose) are not wired into `main.go`.

**`pkg/util`** holds env parsing (`GetEnv`, `GetEnvAsInt`, `GetEnvAsBool`, `GetEnvAsStringSlice`) plus
string/date/struct/random/password helpers, and most of the unit tests.

**Unused by the ETL**: `pkg/aws` (S3), `pkg/httpx`, `pkg/validator`, `pkg/errors`, most of `pkg/logger`, and
`internal/app/consts/{api,file,http_header}.go`.

---

## 11. Dependencies

Go **1.25.3** (`go.mod`), built and tested in the pinned `golang:1.25.3-bookworm` image.

Direct: `jackc/pgx/v5` + `jmoiron/sqlx`, `joho/godotenv`, `gopkg.in/yaml.v3`, `golang.org/x/sync` (errgroup),
`go-playground/validator/v10`, `pressly/goose/v3`, `aws-sdk-go-v2` (+ config/credentials/s3), `golang.org/x/crypto`,
`stretchr/testify`. The Dgraph client (`dgo`) and gRPC left with the incremental mode.

Logging is stdlib `log/slog`; there is no third-party logger.

---

## 12. Build, run, test

No local Go is needed: everything runs through Docker.

```bash
bash build.sh                                  # out/traceability-dgraph-etl (linux/arm64, CGO_ENABLED=0, -trimpath -s -w) + out/config/app.yaml
parity/go.sh test ./...                        # the whole suite (go locally if installed, else the container; PARITY_USE_DOCKER=1 forces it)
parity/go.sh vet ./...
parity/fixture/rebuild.sh                      # synthetic Postgres on 127.0.0.1:55432 (trust auth, tmpfs)
PARITY_REQUIRE_L3=1 parity/go.sh test ./internal/etl/initial -run StampRehearsal -v
```

**The tests need `ETL-Dgraph-Incremental` beside this repository.** `parity/go.sh` mounts both repositories as
`/work/<repo>`.

| Test | What it checks |
|---|---|
| `internal/etl/mode_test.go` | `APP_MODE` parsing and the exact error texts; `Run` rejects other modes |
| `internal/etl/stamp_test.go` | `INITIAL_STAMP_SYNC_TIME` parsing and its error text |
| `internal/etl/initial/schema_test.go` | `schema.dql` byte-identical to the canonical one; the emitted predicates and scalar types equal the schema's, in both directions. **Fails, never skips,** when the sibling schema is missing |
| `internal/etl/initial/sync_time_test.go` | the statements reuse the export filters; `trimSpaceChars` equals `unicode.IsSpace`; one statement per table, events first, links attempted after an events failure; stamping only after the RDF is complete; no `Exec` when disabled; RDF kept and a clear error on failure. Uses the stub driver in `stubdb_test.go` |
| `internal/etl/initial/stamp_rehearsal_test.go` | export + stamp on a **disposable clone** of the fixture database (`CREATE DATABASE … TEMPLATE parity_fixture`, dropped afterwards): every row matches an oracle built from the RDF on disk, and nothing changes with stamping off. Reads `PARITY_PG_URL`; skips without a database unless `PARITY_REQUIRE_L3=1` |
| `internal/etl/common/uid_test.go`, `pkg/util/*_test.go` | the helpers |
| `parity/fixture/gen/main_test.go` | the generator reproduces the committed fixture; every identifier is `SYN-` marked |
| `parity/guards/guards_test.go` | no tracked dumps, credentials or real-looking data; the six frozen fixture files match their pinned SHA-256 |

**Never run a stamping binary or test against `parity_fixture` itself.** The Python tests clone it, and a stamped
template leaves them nothing to process.

Validated on 2026-10-28 (branch `feat/initial-stamp-sync-time`):
- vet and gofmt clean, the whole suite green, the rehearsal green with the template unchanged, and the arm64 build
  succeeds;
- an amd64 end-to-end run of the binary on a clone gave the expected result in every case: a bad setting exits 1 before
  connecting; a read-only role exits 1 with the RDF kept; `false` changes nothing; the default stamps 3 events and 2
  links; a rerun stamps 0.

---

## 13. Deployment and operations

There is no deployment: no Dockerfile, no image, no CI, no ECS task. The output is the standalone `linux/arm64`
binary from `build.sh`, run ad hoc on Linux arm64 (EC2) from a directory holding `config/`.

The bootstrap order is in `REFACTOR_PLAN.md` §9.0 and the Python repository's `deploy/staging/INV7_BULK_LOAD.md`:
1. Run Initial (export + stamp), with `events_bc` / `links_bc` frozen.
2. Confirm the stamp: exit 0, and a `sync_time stamped` log line per table.
3. Run `dgraph bulk` with the Python repository's `schema.dql`.
4. Verify the load.
5. Only then may the incremental ETL run.

The Go incremental image in ECR (with `td-go-rollback.json`) is the rollback artefact of the old incremental job.
Nothing here affects it.

---

## 14. Conventions

- `.editorconfig`: LF, UTF-8, final newline, trimmed trailing whitespace. Go uses tabs (width 2) and a 120-column guide;
  YAML/JSON/DQL/SQL/shell use 2-space indents. `.gitattributes` pins LF, so a Windows checkout (`core.autocrlf`) shows
  CRLF in the working copy only. Run `gofmt` on the committed LF content.
- Errors are wrapped with `fmt.Errorf("context: %w", err)` at every layer; multi-error paths use `errors.Join`.
- Logging is structured `slog` with snake_case keys; the message key is `message`.
- Config structs carry both `yaml` and `json` tags.
- Settings that must not be misread (`APP_MODE`, `INITIAL_STAMP_SYNC_TIME`) are parsed strictly in `internal/etl`, right
  after the config loads and before any connection.
- Commits follow `type: summary` (`feat`, `fix`, `refactor`, `test`, `docs`, `build`, `chore`, `style`).

---

## 15. Knowledge gaps — not verifiable from this repository

1. **No production DDL.** Table definitions live elsewhere; the fixture DDL (`parity/fixture/sql/01_schema.sql`)
   assumes `sync_time timestamptz NULL` and models only the columns read. Index coverage is unknown. The stamp runs
   the same joins as the export (on `events_bc.unique_id`), plus a lookup by `links_bc.id`.
2. **Who sets `sync_time` back to `NULL` and flips `status`.** That is an external system (PD-20).
3. **The stamp's cost and duration at production scale.** It is exercised only on the synthetic fixture (tens of rows).
4. **Where the binary runs** and how `initial_imports/` reaches the bulk-load host. These are open operational items in
   `REFACTOR_PLAN.md`.
5. **Whether the deployed Dgraph schema matches `schema.dql`.** The runbook checks this after a load (INV-5).
6. **Whether `denorm_schema.dql` is aspirational or abandoned.**
7. **Production values** for the worker count and chunk size, and the run duration.
8. **The owner's external flow documentation** referenced in `command.txt` is not in the repository.
