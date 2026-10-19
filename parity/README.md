# Parity harness — P1

The test oracle for the Go → Python port of the incremental ETL (`REFACTOR_PLAN.md` §4, P1-1 … P1-9). It
pins the behaviour of the Go code at baseline **`b487294`** in language-neutral golden files, and gates all
Python work: the port is accepted when it reproduces every golden here.

It adds tests only. No ETL source file differs from `b487294`; `TestParityGoldenProvenance` fails if one
ever does without the goldens being regenerated.

## Commands

All commands run from the repository root. `parity/go.sh` runs `go` locally if installed, otherwise in the
pinned `golang:1.25.3-bookworm` container (`PARITY_USE_DOCKER=1` forces the container).

| What | Command |
|---|---|
| **Rebuild the fixture database from scratch** | `parity/fixture/rebuild.sh` |
| Run everything, L3 required | `PARITY_REQUIRE_L3=1 parity/go.sh test ./...` |
| Run without a database (L1, L2) | `parity/go.sh test ./...` — L3 tests skip |
| Regenerate goldens | `PARITY_REQUIRE_L3=1 parity/go.sh test ./internal/etl/incremental -run 'Parity\|Invariant' -args -update` |
| Regenerate the fixture after editing scenarios | `parity/go.sh run ./parity/fixture/gen`, then `parity/fixture/rebuild.sh` |

`rebuild.sh` starts PostgreSQL 15.17 on `127.0.0.1:55432` with a tmpfs data directory, loads
`fixture/sql/*.sql` and returns once the seed is queryable. Trust auth, localhost only, no credentials.
L3 tests read `PARITY_PG_URL` (`postgres://postgres@127.0.0.1:55432/parity_fixture?sslmode=disable` for a
local Go; `go.sh` sets the container equivalent) and run each test on a fresh `CREATE DATABASE … TEMPLATE`
clone, so the fixture itself is never modified.

`-update` refuses to stamp unless every ETL source (`main.go`, `go.mod`, `go.sum`, `internal/`, `pkg/`,
excluding `*_test.go`) is identical to `HEAD`.

## Layout

```
parity/
  go.sh                      go runner (local or container)
  fixture/
    gen/main.go              generator: the only source of fixture rows
    sql/01_schema.sql        DDL (hand-written)
    sql/02_seed.sql          GENERATED rows for the seven scenarios
    graph_seed.json          GENERATED Dgraph state before the run
    scenarios.json           GENERATED manifest + hand-derived expectations
    compose.yaml, rebuild.sh
  golden/
    l1/*.json                pure functions               (kind l1.cases)
    l2/*.json                RDF writes over plain structs (kind l2.writes)
    l3/sql/*.json            the five SQL strings          (kind l3.rowset)
    l3/pipeline/*.json       full run per scenario         (kind l3.pipeline)
internal/etl/incremental/parity_*_test.go   the Go suite
```

## Golden format — `dgraph-etl-parity/golden/v1`

Every golden is one JSON object (UTF-8, LF):

| Field | Meaning |
|---|---|
| `format` | `dgraph-etl-parity/golden/v1` |
| `kind` | `l1.cases` · `l2.writes` · `l3.rowset` · `l3.pipeline` |
| `name` | path under `golden/` without `.json` |
| `description` | what is pinned, with the Go source location |
| `provenance.generator_commit` | full SHA of `HEAD` when generated; the ETL sources at that commit produced the file |
| `provenance.baseline_commit` | `b487294e04cdb6fea5e49224364830e9762d2f58` |
| `provenance.generated_by`, `go_version` | the Go test and toolchain |
| `comparison` | the comparison rule for this kind, so a consumer needs nothing but the file |
| `data` | the payload |

**`l1.cases`** — `data.cases[]` of `{input, expected, note?, parity_required?}`. Output must equal `expected`
exactly. Cases marked `"parity_required": false` record Go behaviour the port need not copy (one today:
`chunkIndices(0, 0)` panics, unreachable from any call site).

**`l2.writes`** and **`l3.pipeline`** — `data.writes[]` is the ordered list of graph writes, each
`{step, op, set[], del[]}` with `op` one of `MutateRDF`, `MutateWithDeletesRDF`, `DeleteRDF`. Writes are
compared in order; within a write, `set` and `del` are compared in canonical form. Pipelines add:

- `graph_after` — the end state of the graph, keyed by `unique_id`: `predicates` (RDF object text) and
  `links[] {to, facets}`. Nodes with nothing left (deleted) are absent. Compared exactly.
- `sync_time_stamped` — the `events_bc.unique_id`s and `links_bc.id`s whose `sync_time` went from NULL to
  set. Compared exactly.
- `lookups_informational` — the uid lookups issued. **Not part of the contract**; the port may look up
  differently. (The Go suite still compares it, being a Go-vs-Go check.)

Pipelines are generated with every batch and chunk size at 10000 and one worker (`data.config`), which makes
Go's call sequence deterministic. The same run with every size at 1 and four workers must reach the identical
`graph_after` and `sync_time_stamped`; the Go suite checks this for every scenario.

**`l3.rowset`** — the result of one SQL string against the full fixture: `sql` (verbatim) and `sql_sha256`
(the port must use byte-identical SQL), `params`, `columns`, `rows` (sorted by their JSON encoding; compare as
a multiset) and `value_encoding`: each column is rendered as the Go ETL scans it — `nullstring` → string or
null, **untrimmed**; `nullint64` → number or null; `nullfloat64` → the float64 as a decimal string
(`strconv.FormatFloat(v,'f',-1,64)`) or null; `string` → non-null string.

### Canonical form

Applied to each write's `set` and `del` payload independently:

1. Split on `\n`; drop lines that are empty after trimming.
2. Parse each line as `S P O[ (FACETS)] .`. The line must already be exactly that text with single spaces:
   formatting is compared byte-for-byte, not normalised.
3. Rewrite the subject and object terms only (never text inside literals or facets):
   - `<0x…>` → `<uid:UNIQUE_ID>`, the `unique_id` that uid holds in the graph;
   - `_:label` → `_:new:UNIQUE_ID`, where `UNIQUE_ID` is the value of the `_:label <unique_id> "…"` triple in
     the **same** write's `set`. A label without one, two labels for one `unique_id`, or one label for two
     `unique_id`s is an error.
4. Sort the lines bytewise (UTF-8 byte order, which equals code-point order).

So blank-node **label strings are not compared** (P4-3, assessment §2.2); what is enforced is the property:
one label per `unique_id` per mutation, and every triple of a record on that one subject. uid values are not
compared either; only which node they denote. Literal escaping, typed-literal syntax and facet order are
compared byte-for-byte.

## Fixture

Synthetic only (PD-3). Every row is authored in `fixture/gen/main.go`; every identifier carries the `SYN-`
prefix, and `CHECK` constraints in `01_schema.sql` reject any row without it, so nothing extracted from
Staging or Production can be loaded (`TestSeedIsSynthetic`, `TestParityL3FixtureIsSynthetic`).
`TestGeneratedFilesUpToDate` fails if the committed files drift from the generator.

Only the columns the ETL reads are modelled. Types and nullability follow the alembic migration
`Development-Sinarmas/migration-engine/alembic_clean/versions/dev/7661454aa632_add_test_vendor.py`.
**[A]** `sync_time` (timestamptz, nullable) and `mv_subject_hierarchy` (a plain table with the five columns
read; a materialized view in production) are not in that migration and are assumed. Because `process_date`
and `amount` are `NOT NULL` there, their delete path is covered at L2, not L3.

`graph_seed.json` is the Dgraph state assumed already loaded: nodes (`uid`, `unique_id`, `predicates` as RDF
object text) and edges (`from`, `to`, `facets`), tagged by scenario. P4-11 can load it into a local alpha.

### Scenarios (P1-7)

Rows of scenario *Sn* are the `events_bc` rows whose `unique_id`, and the `links_bc` rows whose `event1_id`,
start with `SYN-Sn-`; reference tables are shared. `scenarios.json` holds each scenario's hand-derived
expectations, which the L3 tests assert independently of the goldens.

| # | Scenario | What the run must do | Golden |
|---|---|---|---|
| S1 | New event | create via blank node; the link uses the returned uid | `l3/pipeline/S1_new_event.json` |
| S2 | Updated event | one mutation: changed values set, cleared values `<s> <p> *` (NULL, `''`, whitespace, `{}`, join-cleared) | `S2_updated_event.json` |
| S3 | **Invalidated event** | removal step `DeleteRDF <uid> * * .`; incoming edge rewritten away; never-loaded invalid event stamped without a delete | `S3_invalidated_event.json` |
| S4 | Link added | event1's three links deleted then re-added; empty facets omitted, `qty=0` kept | `S4_link_added.json` |
| S5 | Link invalidated | `invalid` link: id from `removalQuery`, edge dropped; `NULL`-status link: edge dropped, stamped via the links path | `S5_link_invalidated.json` |
| S6 | Event2-only change | event2 upserted; event1 pulled in by `linkEvent1IDsQuery`'s third branch and rewritten | `S6_event2_only_change.json` |
| S7 | Event with no links | nothing written, nothing stamped — stays pending | `S7_event_without_links.json` |
| — | All together | the union, showing no interference | `ALL_all_scenarios.json` |

The removal path cannot be validated on Staging without a restore (assessment §4.2), so it is covered by
S3, `l2/remove_invalid_events.json` and `l2/delete_nodes.json` (P1-5).

## The four silent-corruption invariants (P1-4)

| Invariant | Go source | Tests |
|---|---|---|
| (a) set-or-delete per field, deletes in the same mutation | `events.go:428-638`, `:645`; `graph_ops.go:94-100`; `pkg/dgraph/dgraph.go:346-350` | `TestInvariantA_*` (one subtest per predicate, cross-checked against `schema.dql`) |
| (b) link replacement chunked by `event1_id`, never by link row | `links.go:113-158`, `:67` | `TestInvariantB_LinkReplacementIsGroupedByEvent1` (L2, stub DB, 5 chunk configs × 8 orders); L3 tiny-batch end-state check |
| (c) facets only for non-empty values | `links.go:393-416`, `:338-343` | `TestInvariantC_*` |
| (d) existing uid vs newly minted blank node | `events.go:416-423`, `:650-654`; `links.go:325-332` | `TestInvariantD_*` |

### Mutation evidence

Each invariant was broken deliberately in a scratch copy (never in this repository) and the suite run with L3
enabled; the unmodified copy passed. Generated against `b487294` on 2026-10-18.

| Mutant (exact change) | Invariant tests that failed |
|---|---|
| a1 — drop `AppendTriple(&delBuilder, subject, "<plant>", "*")` | `TestInvariantA_EachClearedField…/plant`, `…AllClearedAndAllPresent`, `…MixedBatchStaysOneMutation`, `…ClearedValueIsRemovedFromGraph` |
| a2 — send `set` via `MutateRDF`, then `del` via a separate `DeleteRDF` | `TestInvariantA_EachClearedField…` (all 42), `…AllClearedAndAllPresent`, `…MixedBatchStaysOneMutation` |
| b — after the per-chunk delete, re-run delete+upsert per `FetchChunkSize` slice of link **rows** | `TestInvariantB_…` (4 of 5 configs: E1 kept `->Z` only); L3 tiny-batch end-state check (S4, ALL) |
| c — `if true \|\| record.Role != ""` | `TestInvariantC_FacetEmittedIffValuePresent`, `TestInvariantC_NoParenthesesWithoutFacets` |
| d1 — `false && ok && uid != ""` (always blank node) | `TestInvariantD_SubjectSelection`, `TestInvariantD_BlankNodeUIDRoundTrip` |
| d2 — drop `uids.Set(id, uid)` (returned uids not merged) | `TestInvariantD_BlankNodeUIDRoundTrip` |

Every mutant also failed the relevant goldens. d2 is caught by the contract only at L2: in a full run the
links step re-looks up the missing uid and recovers, so the S1 pipeline golden differs only in
`lookups_informational` (one extra lookup), which a port's comparison ignores. That is why the L2 round-trip
test exists.

## Notes for the Python port (P3/P4)

- **INV-3 was answered YES** (P0-4): pydgraph carries `set_nquads` and `del_nquads` in one mutation. Every
  `MutateWithDeletesRDF` write here stays a single mutation in P4-2; **no golden needs a two-mutation
  treatment.**
- Go emits `<s> <p> * .` with a **blank-node subject** in `del_nquads` for a new event's empty fields
  (S1: `_:new:SYN-S1-E01 <version> * .`). P0's INV-3 spike did not cover blank nodes in `del_nquads`; check it
  against the v25.3.1 alpha in P4.
- String literals are Go `strconv.Quote`, not `json.dumps` or `repr` (`l1/string_literal.json`): non-ASCII
  printable stays raw; `\x00`-style and ` `-style escapes follow `strconv.IsPrint`.
- Floats are `FormatFloat(v,'f',-1,64)`: never an exponent (`1e21` → `1000000000000000000000`), `-0` kept,
  `NaN`/`+Inf` possible from `numeric` (`l1/float_literal.json`).
- Trimming is Go `strings.TrimSpace`; Python `str.strip()` additionally strips U+001C–U+001F
  (`l1/normalize_string.json`).
- `src_path` is `jsonb`: the ETL uses PostgreSQL's text rendering (keys reordered, `1.10` kept, raw UTF-8 —
  see `l3/sql/event_details_query.json` and S1). psycopg returns a `dict` by default; re-serialising it will
  not match. Fetch it as text.
- `amount`/`qty` are `numeric` scanned to float64: `12345678901234567890.125` becomes
  `12345678901234567000`.

## Observed Go behaviour, recorded as specification

Encoded as found at `b487294`, not judged. Raised for the owner:

1. **Events with no valid link are never synced and never stamped** (S7): `eventIDsQuery` requires a valid
   link, so such an event stays `sync_time IS NULL` and is re-scanned every run until a link appears.
2. **Links whose endpoint has no uid are dropped silently but stamped** (`links.go:325-329` skips them;
   `linkIDs` includes every fetched row). Such a link is never written unless its row is re-nulled.
3. **Malformed lookup responses read as "not found"** (`helpers.go:85-87`): the event would then be created
   as a new node, duplicating its `unique_id`.
4. **Removal leaves incoming edges until the links step rewrites their event1** (S3, and visible as a
   dangling edge in `l2/remove_invalid_events.json`).
5. **Not encoded — open question:** if `mv_subject_hierarchy` (or `master_mill` beyond its `GROUP BY`) ever
   has several rows for one key, `eventDetailsQuery` returns one event several times. The repeats land in one
   mutation (where a set and a delete of the same predicate resolve as delete-wins, INV-3) or in different
   batches (order-dependent). The fixture keeps one hierarchy row per key; whether production guarantees that
   is unknown.

## Synthetic-data and credential guards (P4-14)

`parity/guards/guards_test.go` - ordinary tests in the default `go test ./...` (no tag, no flag, no CI).
They read only this checkout: the tracked-file list is parsed from `.git/index` (no git binary), and
nothing touches the network, AWS or a credential.

| Check | What fails it |
|---|---|
| (a) | a tracked `*.sql`, `*.dump`, `*.csv`, `*.rdf.gz` (and similar extract formats) outside `parity/fixture/` |
| (b) | a tracked `.env*` (other than `.env.example`), key or credential file; an AWS-key, private-key, token or password-in-URL shape, or a random-looking literal, in tracked text (`go.sum` exempt: module checksums) |
| (c) | a seed row (`sql/02_seed.sql`, parsed) whose identifier column lacks `SYN-`, a link `id` outside the 9S000k block, a `graph_seed.json` node or edge without `SYN-`, or a real-data look-alike (email, UUID, IP, AWS ARN, Indonesian phone / NPWP, company legal form, real organisation name) in the fixture or the goldens |
| (d) | not applicable here: the Go code has no Secrets Manager loader. The §8.5 key-contract check lives in ETL-Dgraph-Incremental |

Each check has a `TestNegative*` that plants what it forbids. The same rules are implemented in
ETL-Dgraph-Incremental (`tests/guards.py`). Its `scripts/demo_guards_offline.sh` also runs this
repository's suite with `--network none`.
