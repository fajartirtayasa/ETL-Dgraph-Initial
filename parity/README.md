# parity/ — the frozen fixture and the guards

This directory holds what the Python repository (`ETL-Dgraph-Incremental`) and this repository still share:

- **`fixture/`** — the synthetic PostgreSQL fixture. It is **frozen**: the Python repository runs it in place and pins
  its SHA-256 values in `tests/fixture.lock`.
- **`guards/`** — the synthetic-data and credential guards, and the checksum guard that freezes the fixture.
- **`go.sh`** — runs `go` in the pinned container with both repositories mounted.

The **golden corpus** that used to be here (`parity/golden/`, 28 files) is gone. It pinned the behaviour of the Go
incremental ETL at baseline commit **`b487294`** and gated the Go → Python port. It lives on in the Python repository
(`tests/golden/`, a byte-for-byte copy, with `MANIFEST.sha256`) and in this repository's history (commit `79a38f0`
added it). The golden format specification is `tests/golden/FORMAT.md` there, which is this README as it stood at
`79a38f0`. The Go incremental code and its parity suite were removed with the incremental mode, so the goldens can
no longer be regenerated; the fixture is frozen for the same reason.

## Commands

All commands run from the repository root. `parity/go.sh` runs `go` locally if installed, otherwise in the pinned
`golang:1.25.3-bookworm` container (`PARITY_USE_DOCKER=1` forces the container). It needs `../ETL-Dgraph-Incremental`
and mounts both repositories side by side as `/work/<repo>`; it stops with an error if the sibling is missing.

| What | Command |
|---|---|
| Run everything | `parity/go.sh test ./...` |
| Only the guards | `parity/go.sh test ./parity/guards/` |
| Only the schema tests | `parity/go.sh test ./internal/etl/initial -run 'Schema\|Emitted' -v` |
| Check the generator still reproduces the fixture | `parity/go.sh test ./parity/fixture/gen` |
| Start the fixture database (for the Python repository) | `parity/fixture/rebuild.sh` |

`rebuild.sh` starts PostgreSQL 15.17 on `127.0.0.1:55432` with a tmpfs data directory, loads `fixture/sql/*.sql` and
returns once the seed is queryable. Trust auth, localhost only, no credentials. The Go suite no longer reads a
database; `go.sh` still forwards `PARITY_PG_URL` and `PARITY_REQUIRE_L3`, which nothing in it uses any more.

## Layout

```
parity/
  go.sh                      go runner (local or container, both repositories mounted)
  fixture/                   FROZEN - do not edit any file here
    gen/main.go              generator: the only source of fixture rows (kept as a reproducibility check)
    gen/main_test.go         TestGeneratedFilesUpToDate, TestSeedIsSynthetic
    sql/01_schema.sql        DDL (hand-written)
    sql/02_seed.sql          GENERATED rows for the seven scenarios
    graph_seed.json          GENERATED Dgraph state before the run
    scenarios.json           GENERATED manifest + hand-derived expectations
    compose.yaml, rebuild.sh
  guards/guards_test.go      the guards below
```

## The frozen fixture

Six files are pinned by SHA-256 in `parity/guards/guards_test.go` (`fixtureLock`) and, with identical values, in the
Python repository's `tests/fixture.lock`:

| File (under `parity/fixture/`) | SHA-256 |
|---|---|
| `sql/01_schema.sql` | `ecaab79678dab1e34e9bccd218d5d9e111cf2659be6ea66968f576c631ecd5cd` |
| `sql/02_seed.sql` | `62df94ff0139b71e44dc7bc83bb93597e251fbc0b1e1bd059376a6d5ec16fc03` |
| `graph_seed.json` | `bd7729cc5f176d9e3902b71932be2bfca7357b6e24049516cbbd6bb3edd77799` |
| `scenarios.json` | `1deeb55a4e0fbbc08e2c87b4bcde878bbc86cf2a60a7f034f47464fa1f0bb9b6` |
| `compose.yaml` | `4a47fe237d949974b2d43f9a6ececbff9ba13a42582b5f4c2de831a26a9deb83` |
| `rebuild.sh` | `4625c84d6ea7edeaaa18691e1205a76a173049538b7daefb6a8d92f0d49000e1` |

A change to any of them fails `TestGuardE_FrozenFixtureMatchesItsPinnedChecksums` here and the Python repository's
`scripts/fixture.sh` (exit 3). Both locks would have to change together, which the freeze forbids. That includes
text inside these files that cites the plan (`REFACTOR_PLAN.md`): it refers to the Dgraph repository's copy by
convention and stays as it is.

**The generator is a reproducibility check, not a way to change the fixture.** `parity/fixture/gen` renders the three
generated files; `TestGeneratedFilesUpToDate` fails if the committed bytes differ from its output. If it ever fails,
something drifted: restore the committed files, do not regenerate and commit.

### What the fixture is

Synthetic only (PD-3). Every row is authored in `fixture/gen/main.go`; every identifier carries the `SYN-` prefix,
and `CHECK` constraints in `01_schema.sql` reject any row without it, so nothing extracted from Staging or Production
can be loaded (`TestSeedIsSynthetic`). Only the columns the ETLs read are modelled. Types and nullability follow the
alembic migration `Development-Sinarmas/migration-engine/alembic_clean/versions/dev/7661454aa632_add_test_vendor.py`.
`sync_time` (timestamptz, nullable) and `mv_subject_hierarchy` (a plain table with the five columns read; a
materialized view in production) are not in that migration and are assumed.

`graph_seed.json` is the Dgraph state assumed already loaded before an incremental run: nodes (`uid`, `unique_id`,
`predicates` as RDF object text) and edges (`from`, `to`, `facets`), tagged by scenario.

Rows of scenario *Sn* are the `events_bc` rows whose `unique_id`, and the `links_bc` rows whose `event1_id`, start
with `SYN-Sn-`; reference tables are shared. `scenarios.json` holds each scenario's hand-derived expectations.

| # | Scenario | What an incremental run must do |
|---|---|---|
| S1 | New event | create via blank node; the link uses the returned uid |
| S2 | Updated event | one mutation: changed values set, cleared values `<s> <p> *` (NULL, `''`, whitespace, `{}`, join-cleared) |
| S3 | **Invalidated event** | removal step `DeleteRDF <uid> * * .`; incoming edge rewritten away; never-loaded invalid event stamped without a delete |
| S4 | Link added | event1's three links deleted then re-added; empty facets omitted, `qty=0` kept |
| S5 | Link invalidated | `invalid` link: edge dropped; `NULL`-status link: edge dropped, stamped via the links path |
| S6 | Event2-only change | event2 upserted; event1 pulled in and rewritten |
| S7 | Event with no links | nothing written, nothing stamped — stays pending |

These expectations describe the incremental ETL, which is now the Python repository's concern; the initial export in
this repository does not run them.

## The tests in this repository

| Where | What |
|---|---|
| `internal/etl/mode_test.go` | `APP_MODE` parsing: only `initial` (trimmed, case-insensitive) is accepted; the exact error text for every rejection |
| `internal/etl/initial/schema_test.go` | the mirror test (`schema.dql` byte-identical to the canonical one) and the predicate test (the export emits exactly the schema's predicates and types, plus `links`); reads `../ETL-Dgraph-Incremental/schema.dql` and fails if it is missing |
| `parity/fixture/gen/main_test.go` | the generator reproduces the committed fixture; every seed identifier is `SYN-` marked |
| `parity/guards/guards_test.go` | the guards below |
| `internal/etl/common`, `pkg/util` | unit tests of the helpers |

## Synthetic-data and credential guards (P4-14) and the frozen-fixture guard

`parity/guards/guards_test.go` holds ordinary tests in the default `go test ./...` (no tag, no flag, no CI). They read
only this checkout: the tracked-file list is parsed from `.git/index` (no git binary), and nothing touches the
network, AWS or a credential.

| Check | What fails it |
|---|---|
| (a) | a tracked `*.sql`, `*.dump`, `*.csv`, `*.rdf.gz` (and similar extract formats) outside `parity/fixture/` |
| (b) | a tracked `.env*` (other than `.env.example`), key or credential file; an AWS-key, private-key, token or password-in-URL shape, or a random-looking literal, in tracked text (`go.sum` exempt: module checksums) |
| (c) | a seed row (`sql/02_seed.sql`, parsed) whose identifier column lacks `SYN-`, a link `id` outside the 9S000k block, a `graph_seed.json` node or edge without `SYN-`, or a real-data look-alike (email, UUID, IP, AWS ARN, Indonesian phone / NPWP, company legal form, real organisation name) in the fixture |
| (d) | not applicable here: the Go code has no Secrets Manager loader. The §8.5 key-contract check lives in ETL-Dgraph-Incremental |
| (e) | any of the six frozen fixture files differing from its pinned SHA-256 (above) |

Each check has a `TestNegative*` that plants what it forbids; (e)'s copies the fixture into a temporary directory and
flips one byte. The same rules are implemented in ETL-Dgraph-Incremental (`tests/guards.py`). Its
`scripts/demo_guards_offline.sh` also runs this repository's suite with `--network none`.
