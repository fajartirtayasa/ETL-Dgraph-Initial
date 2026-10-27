# Plan: stamp `sync_time` after the initial export

Status: approved by the owner on 2026-10-28; being implemented on branch `feat/initial-stamp-sync-time` here and
`docs/initial-stamp-sync-time` in `Dgraph`. Nothing is pushed or merged until the owner reviews it. Delete this file
before merging.

## Decisions (owner, 2026-10-28)

| # | Decision |
|---|---|
| S1 | After both RDF writers close successfully, the binary runs **one `UPDATE` per table** (`events_bc`, then `links_bc`), built from **the same SQL filters as the export**. No record IDs are collected. Bulk loading stays manual. |
| S2 | Only exported rows that have `status = 'valid'` and `sync_time IS NULL`. Links are identified by `links_bc.id`. |
| S3 | The owner guarantees that `events_bc` and `links_bc` do not change from the start of the export to the end of the stamping, so the filters select the same rows both times. |
| S4 | If stamping fails, keep the RDF files, log a clear error and exit non-zero. Nothing re-exports automatically. Recovery is to re-run the stamping statements while the freeze still holds. |
| S5 | `INITIAL_STAMP_SYNC_TIME`, default `true`. `false` skips stamping. A value that is not a boolean stops the binary at startup, before it connects to PostgreSQL. |
| S6 | Stamping is tested only on a disposable clone (`CREATE DATABASE … TEMPLATE parity_fixture`), never on `parity_fixture` itself. |
| S7 | Update `CLAUDE.md` and the affected sections of `Dgraph/REFACTOR_PLAN.md`. |

## Code

- `internal/etl/initial/events.go`, `links.go`: move the export filters into shared constants.
  - `exportedLinksFilter`: the `FROM … JOIN … WHERE` that both `linksQuery` and `UnsyncedLinks` use.
  - `exportedEventIDsCTE`: `UnsyncedLinks` plus `UnsyncedUniqueIDs`.
  - The queries return the same rows as before; only whitespace changes.
- `internal/etl/initial/sync_time.go` (new):
  - `stampEventsStatement`: `exportedEventIDsCTE` + `UPDATE events_bc … WHERE unique_id IN UnsyncedUniqueIDs`.
  - `stampLinksStatement`: `UPDATE links_bc … WHERE id IN (SELECT l.id` + `exportedLinksFilter` + `)`.
  - Both statements also require `status = 'valid' AND sync_time IS NULL`, and skip IDs that are blank after
    `strings.TrimSpace`, as the export does. The blank check is `btrim(…, $1) <> ''`, where `$1` is the exact set of
    characters `unicode.IsSpace` matches.
  - `stampSyncTime` runs events then links. Each statement commits on its own, links run even if events failed, and
    both errors are joined. It logs the number of rows each statement stamped.
- `internal/etl/initial/migrator.go`: add the option `WithSyncTimeStamping(bool)`, off unless the caller sets it.
  `Run` stamps only after `streamLinks` returns nil. On failure it returns an error that says the RDF is complete and
  kept, and names the recovery statements.
- `internal/etl/stamp.go` (new): `ParseStampSyncTime`. Empty means `true`, `strconv.ParseBool` values are accepted
  (trimmed, any case), anything else is an error.
- `internal/etl/runner.go`, `main.go`: parse the setting right after `APP_MODE` and pass it through
  `RunnerConfig.StampSyncTime`.

## Configuration (the three files that must stay in sync)

- `config/app.yaml`: a new `initial:` section with `stamp_sync_time: "true"` and a comment.
- `internal/app/config.go`: `InitialConfig.StampSyncTime string`, read with `util.GetEnv`. It is a string so that
  empty means the default and an invalid value is an error, not silently replaced by the default.
- `.env.example`: `INITIAL_STAMP_SYNC_TIME=` (blank means `true`).

## Tests

- `internal/etl/stamp_test.go`: every accepted and rejected value, and the exact error text.
- `internal/etl/initial/sync_time_test.go` (unit tests, using the stub driver):
  - the blank-ID character set equals `unicode.IsSpace` across all runes;
  - both statements contain the shared export filters and the `status` / `sync_time` conditions;
  - one `Exec` per table, events first, with links still attempted after an events failure;
  - `Run` stamps only once both files are complete;
  - `Run` with stamping disabled never calls `Exec`;
  - `Run` keeps both RDF files and returns the clear error when stamping fails.
- `internal/etl/initial/stubdb_test.go`: `Exec` answered by an optional function.
- `internal/etl/initial/stamp_rehearsal_test.go` (live database):
  - Clones `parity_fixture` into a disposable database, runs `Migrator.Run` on the clone, and checks every
    `events_bc` / `links_bc` row against an oracle built from the RDF it wrote and the rows before the run. Exactly
    the exported, valid, unstamped rows change; nothing else does.
  - A second clone with stamping disabled must not change at all.
  - The clone is dropped afterwards. The test refuses to stamp `parity_fixture`.
  - It reads `PARITY_PG_URL`. Without a reachable database it skips, unless `PARITY_REQUIRE_L3=1` is set, the existing
    convention of `parity/go.sh`. That keeps the Python repository's offline run (`--network none`) passing.

## Documentation

- `CLAUDE.md`: what the binary does (the stamp is its only write to PostgreSQL), the run sequence, failure and
  recovery, the new setting, and the live test.
- `parity/README.md`, `parity/go.sh`: `PARITY_PG_URL` / `PARITY_REQUIRE_L3` are used again, by the rehearsal test.
- `Dgraph/REFACTOR_PLAN.md`:
  - PD-18 is superseded.
  - Update §9.0.1 (statements, scope, ordering, recovery), P6-0a, P6-0c, P6-0e, §9.0.2, §9.0.4 step 5, the §10
    readiness line, P7-12 and the line-14 "feature-frozen" wording, and risks R-1265 and R-1266.
  - Add the new decisions to §14.1 (M34 onward).

## Validation (needs Docker; Docker is off until the owner starts it)

1. `PARITY_USE_DOCKER=1 parity/go.sh vet ./...` and `gofmt -l` on the changed files.
2. `PARITY_USE_DOCKER=1 parity/go.sh test ./...` with the fixture down: everything passes, and the rehearsal test skips.
3. `parity/fixture/rebuild.sh`, then:
   - record the `parity_fixture` counts (`sync_time IS NULL`) for both tables;
   - run `PARITY_REQUIRE_L3=1 PARITY_USE_DOCKER=1 parity/go.sh test ./internal/etl/initial -run StampRehearsal -v`;
   - re-check that the `parity_fixture` counts are unchanged;
   - shut the fixture down.
4. `bash build.sh`: the binary builds, and `out/config/app.yaml` carries `stamp_sync_time`.
5. `git diff main -- parity/fixture` is empty. The guards, including the fixture checksum guard, run in step 2.
