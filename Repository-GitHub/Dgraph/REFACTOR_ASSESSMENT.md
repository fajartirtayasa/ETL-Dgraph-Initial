# REFACTOR_ASSESSMENT.md

Assessment and migration plan: `traceability-dgraph-etl`.

**Status — decision taken (scope: partial migration).** The repository owner has chosen:

> Retain Go. Partial migration (`incremental` → Python, `initial` stays Go) is acceptable, but only after building a parity harness.

Together with the owner's earlier two mitigations — **build ETL tests first** and **deploy to Staging before Production** — this document is the plan for that scope. It is no longer a go/no-go analysis.

**Scope** — analysis and planning only. No code or configuration was modified; no migration was implemented.

**Evidence markers:** **[V]** verified in this repository (file/line) · **[X]** verified outside it (pydgraph source/PyPI, sibling repos) · **[A]** assumption · **[R]** recommendation.

See `BASE_KNOWLEDGE.md` for the architectural detail this builds on.

---

## 1. Executive summary

**[R] The chosen scope is the right one, and it is cleaner than it first appears.** Verification of the module boundary turned up two findings that make the split cheaper than the earlier full-migration plan:

| Finding | Consequence |
|---|---|
| **[V] `initial` mode has no Dgraph dependency at all.** `grep` confirms `pkg/dgraph` is imported only by `incremental/`, `runner.go`, and `main.go` — never by `internal/etl/initial` or `internal/etl/common`. **[V]** `main.go:67` constructs the graph adapter only when mode is incremental. | The **retained Go binary drops the entire Dgraph client**. It becomes a pure Postgres→RDF-file exporter: ~778 lines of ETL logic plus `common/` and `pkg/database`. The Go surface the team must maintain shrinks sharply. |
| **[V] Blank-node labels are never shared across the two programs.** `initial`'s labels are internal to one bulk load (`events.rdf.gz` ↔ `links.rdf.gz`); `incremental`'s are internal to a single mutation, mapped back via `blankToID`. | **Byte-parity of `BlankNode` across languages is not required.** This drops from a High risk to a Low one, and the Python port can use a simpler mutation-local labelling scheme (§4.2). |
| **[V]** Only **4 Dgraph operations** are used — `RunDQL` (read-only + best-effort), `MutateRDF`, `MutateWithDeletesRDF`, `DeleteRDF`. **[X]** pydgraph 25.2.0 covers all four. | Client layer is not a risk. |
| **[V]** The RDF-generation core takes plain structs, not DB handles. | The highest-value tests need **zero infrastructure** (§3). |

**[R] What this scope eliminates:** the `initial` mode performance risk — previously rated High and resting on an unmeasured **[A]** 10–40× CPython estimate — disappears entirely, because that code is not ported. No benchmark gate is needed.

**[R] What survives:** the memory risk in `incremental` (**[V]** every unsynced ID is materialised in memory before chunking), the Staging-scale question, and a verified trap in shadow-running (§3.3). Plus one new permanent cost: a two-language estate.

---

## 2. Target architecture

```
┌─ Go (retained, feature-frozen) ──────────────────────────────┐
│  main.go (initial path) · internal/app · pkg/database        │
│  internal/etl/common · internal/etl/initial                  │
│  → streams Postgres → chunked gzip RDF → initial_imports/    │
│  NO Dgraph client  [V]                                        │
└──────────────────────────────────────────────────────────────┘
┌─ Python (new) ───────────────────────────────────────────────┐
│  config · psycopg3 · pydgraph                                │
│  incremental: check → removal → events → links → sync_time   │
│               → matview refresh                               │
│  → mutates live Dgraph, stamps sync_time in Postgres         │
└──────────────────────────────────────────────────────────────┘

Shared contract (narrow, stable):
  schema.dql predicates · "Event" type · "links" edge + 6 facet names
  unique_id as business key · mutual exclusion at runtime
```

**[R] The Go binary should be feature-frozen, not merely retained** — it keeps running `initial` as a rare bootstrap (**[V]** `command.txt`: "already stable"), and all future business-logic change lands in Python.

### 2.1 What is genuinely shared — and what is not

**[R] Good news for this split: the two programs are more decoupled than a shared-codebase intuition suggests.**

| Candidate coupling | Actually shared? | Basis |
|---|---|---|
| `schema.dql` predicate names, `Event` type, `links` edge, 6 facet names | **Yes — permanent contract** | **[V]** both write the same predicates |
| `unique_id` as the business key | **Yes** | **[V]** lookups key on it, `@upsert` in schema |
| Runtime mutual exclusion (never both writing the graph) | **Yes — operational** | **[V]** `initial` rebuilds; `incremental` mutates + stamps `sync_time` |
| `BlankNode` byte-for-byte output | **No** | **[V]** labels are load-local / mutation-local (§1) |
| Literal formatting (`strconv.Quote`, XSD int/float) | **No** | **[A]** differing-but-valid literal text parses to the same stored value; each side need only emit *valid* RDF, enforced by its own tests |
| `sync_time` stamping | **No** | **[V]** only `incremental` touches it |

**[R] Required of the Python port instead of byte-parity:** emit syntactically valid N-Quads, and guarantee that two distinct `unique_id`s never collapse to the same blank-node label *within one mutation batch*. Go achieves the latter via sanitise + SHA-1 suffix; Python needs the property, not the algorithm.

---

## 3. Parity harness (the precondition)

**[R] This is the gating deliverable. Nothing Python starts until it is green.**

### 3.1 Critical sequencing

**[R] The harness must be written against the current Go code first.** Tests written after the port would certify the port against itself. Golden files generated from Go runs are language-neutral artifacts and become the Python port's acceptance criteria.

### 3.2 Four layers, ordered by value-per-cost

**[V]** Seam analysis shows the testable surface is far better than the zero-coverage figure implies.

| Layer | Covers | Infrastructure | Verified seam | Effort |
|---|---|---|---|---|
| **L1 — Pure functions** | RDF literals, `buildLinkFacets`, `chunkIndices`, interner, normalizers | **None** | package-level / struct-only funcs | **S** |
| **L2 — RDF payload golden files** | Set-or-delete field semantics, facets, blank-node vs uid subject choice, batching | **None** — a fake `dgraph.Adapter` captures the RDF strings | **[V]** `upsertEvents(ctx, []eventRecord, *uidMap)`, `upsertLinks(...)`, `deleteExistingLinks(...)` take plain structs; **[V]** adapter returns plain types (`*DQLResponse`, `map[string]string`, `error`) | **M** |
| **L3 — SQL contract** | The CTE queries — which rows each selects | **Real Postgres** fixture | **[V]** `QueryRows` returns concrete `*sql.Rows`; cannot be meaningfully faked | **M** |
| **L4 — End-to-end** | Full `incremental` run: step ordering, `sync_time` stamping, matview refresh, graph state | Postgres + Dgraph | whole-binary | **L** |

**[R] L2 is the highest-value layer and needs no infrastructure.** It pins exactly the invariants that corrupt data silently:
- set-or-delete per field (a cleared Postgres value must emit `<s> <p> *`),
- chunk-by-`event1_id` link replacement,
- facets emitted only for non-empty values,
- existing-uid vs new-blank-node subject selection.

**[R] L3 must use a real database, not mocks.** **[V]** Much of the business logic lives *in the SQL* — `eventIDsQuery`, `linkEvent1IDsQuery`, `removalQuery` are multi-CTE statements encoding the whole change-detection policy. Mocking `*sql.Rows` would test the scanner and assert nothing about correctness.

**[R] L3 ports for free.** The SQL strings are identical in Go and Python, so the same fixture + expected-rowset pairs validate both implementations unchanged. **[R] Scope L1–L3 to `incremental` only** — `initial` is not being ported, so its tests are optional (though L2 coverage of `emitEvent` is cheap insurance for a feature-frozen binary).

### 3.3 Shadow-running is not free — a verified constraint

**[V] The incremental ETL mutates its own source of truth.** `sync_time.go` runs `UPDATE ... SET sync_time = NOW() WHERE sync_time IS NULL`; `materialized_views.go` refreshes three matviews; Dgraph mutations are destructive.

**Consequence:** you **cannot** run the Go binary then the Python binary against the same Staging database and compare. The first run stamps `sync_time`, so the second sees an empty changeset and does nothing — it would look identical and prove nothing.

| Option | Mechanism | Cost |
|---|---|---|
| **Snapshot/restore** *(recommended)* | `pg_dump`/restore Staging + reset Dgraph between runs; diff both end states | Moderate; most faithful |
| **Separate Dgraph namespace** | Each binary against its own namespace from a shared DB snapshot | **[X]** pydgraph supports namespaces; **[V]** Go code never calls them — needs new code |
| **Read-only dry-run flag** | Suppress writes; compare intended mutations | Requires changing Go code — out of scope per constraints |

**[R] Decide before Phase 3.** This is the difference between a meaningful Staging validation and a false green.

---

## 4. Component matrix

Effort: **S** ≤ 3 days · **M** ~1–2 weeks · **L** ~3–6 weeks. **[A]** One experienced developer, functional parity, excluding validation.

| Component | Verdict | Effort | Reasoning |
|---|---|---|---|
| `internal/etl/initial` (778 lines) | **Retain in Go — feature-freeze** | — | **[R]** Rare bootstrap, worst CPython profile, lowest change frequency. Not ported. |
| `internal/etl/common` | **Retain in Go** *(Go copy)* + **reimplement in Python** | M | **[V]** Needed by `initial`. Python needs its own RDF-literal/writer equivalents — **[V]** no byte-parity required (§2.1). |
| `pkg/database` | **Retain in Go** *(Go copy)* + **reimplement in Python** | S | **[R]** Python side: `psycopg3` (server-side cursors + async), departing from the team's usual `psycopg2` **[X]**. |
| `internal/app` config | **Retain** + **reimplement** | S | **[R]** `pydantic-settings`; matches sibling-repo `config.yaml` convention **[X]**. |
| `internal/etl/incremental` (1,893 lines) | **Migrate to Python** | L | **[R]** The migration. I/O-bound → smallest Python penalty; runs daily; carries the subtle invariants. |
| `pkg/dgraph` | **Delete from Go binary** → Python owns | M | **[V]** Imported only by `incremental`, `runner.go`, `main.go`. Once incremental leaves, the Go binary needs no graph client. Python must re-implement the retry classifier (**[V]** `isRetryableError`, ~18 patterns) and resolve the gzip gap (Q4). |
| `main.go`, `internal/etl/runner.go`, `mode.go` | **Simplify in Go** | S | **[R]** Drop the incremental branch and the two-mode dispatch; the Go binary becomes single-purpose. |
| `pkg/util` | **Retain subset in Go**; **drop for Python** | S | **[V]** Still used by the Go binary. Python uses stdlib + `pydantic-settings`. |
| `pkg/aws`, `pkg/httpx`, `pkg/validator`, `pkg/errors`, most of `pkg/logger` | **Drop from both** | S | **[V]** Unused by any ETL path. |
| `common.StringInterner` | **Investigate** (Python side) | S | **[V]** Exists for memory at scale. **[A]** Python's economics differ. Needs volume data (Q2). |
| `deployment/` | **Extend** | M | **[R]** Second image + pipeline for the Python service; Go pipeline stays. |
| `schema.dql` | **Unaffected** | — | **[V]** Applied manually; not loaded by either binary. |

---

## 5. Roadmap

### Phase 0 — Parity harness *(gating; in Go)*

Write L1 + L2 against the **current Go code**; export L2 payloads as golden files. Stand up the L3 fixture DB seeded with: new event, updated event, invalidated event, link added, link invalidated, event2-only change, event with no links.

**Exit:** golden files committed; `go test ./...` green; fixture DB reproducible from a script.
**[A] Effort:** 2–4 weeks. **[R] Standalone value** — it also de-risks any future change to the retained Go binary.

### Phase 1 — Python periphery + spikes

Config, logging, `psycopg3` wrapper, pydgraph wrapper. Resolve Q3 (`set_nquads` + `del_nquads` in one mutation) and Q4 (gRPC gzip). Port L1 tests.

**Exit:** L1 parity green; both spikes answered.

### Phase 2 — Port `incremental`

**[R]** In dependency order, each step validated against its Phase-0 golden files before moving on:
`traceability_check` → `removal` → `events` → `links` → `sync_time` → `materialized_views`.
**[R]** `links` last — it carries the chunk-by-`event1_id` invariant, the subtlest rule in the codebase.

**Exit:** L2 golden files reproduced by Python; L3 queries return identical rowsets.

### Phase 3 — Staging validation

Resolve the shadow-running mechanism (§3.3) **first**. Then run Go and Python against identical restored Staging snapshots and diff both end states (Postgres `sync_time` sets + Dgraph graph state).

**[A] Exit — owner to confirm (Q5):** ≥5 consecutive daily cycles, zero diffs.

### Phase 4 — Production cutover + Go decommission

Deploy the Python incremental service to Production; retain the Go image for rollback. **[R]** Monitor via the `sync_time` watermark — a silent failure leaves rows unstamped, detectable by `WHERE sync_time IS NULL` counts before and after.

Once stable, strip the incremental path and `pkg/dgraph` from the Go binary, leaving the frozen `initial` exporter.

**[A] Indicative total:** Phase 0 ~2–4 weeks; Phases 1–3 ~8–12 weeks; Phase 4 ~2–3 weeks. **~3 months, one developer** — roughly 1–2 months less than the full-migration scope, with validation as the long pole.

---

## 6. Risks and open questions

### Risk register under this scope

| Risk | Severity | Status |
|---|---|---|
| **`initial` mode performance** | — | **Eliminated** by scope. Not ported. |
| **`BlankNode` cross-language divergence** | **Low** | **[V]** Downgraded from High — labels are load-/mutation-local (§2.1). |
| **No test oracle** | **Low** | Mitigated by Phase 0, **provided** tests are written against Go first. |
| **Silent graph corruption from a porting error** | **Medium** | Phase 0 catches logic drift; Phase 3 catches integration drift. |
| **Memory blow-up in `incremental`** | **High** | **Not mitigated.** **[V]** `events.go:177`, `links.go:113` materialise every unsynced ID. **[A]** Python's per-string overhead may force a redesign (keyset pagination), not a translation. Needs Q2. |
| **Staging ≠ Production scale** | **High** | **Not mitigated.** **[A]** If Staging is small, Phase 3 proves correctness but nothing about throughput or memory. |
| **Two-language estate (permanent)** | **Medium** | **[R]** New, accepted cost. Mitigated by feature-freezing Go and the narrow shared contract (§2.1). |
| **Fixture realism** | **Medium** | **[R]** Golden files cover only seeded scenarios; an unanticipated production data shape is caught by neither tests nor Staging. |
| **No in-house pydgraph experience** | **Medium** | **[X]** `caching-tree-molecular/by_dgraph` is named for a *caching strategy*, not a Dgraph client — it uses `psycopg2`/`requests`. No Python↔Dgraph integration exists anywhere in this workspace. |
| **Zero git history** | **Medium** | **[V]** No record of past fixes; a prior bug could be silently reintroduced. |

### Assumptions

- **[A]** Daily `incremental` volume ≪ `initial` full export. Plausible from the `sync_time IS NULL` design; unconfirmed.
- **[A]** One experienced developer; estimates exclude review and change-management overhead.
- **[A]** Staging and Production share schema and Dgraph version.
- **[A]** `initial` will run rarely enough that a frozen Go binary is acceptable indefinitely.

### Open questions

| # | Question | Blocks |
|---|---|---|
| **Q1** | **Row counts, runtimes, and the batch window for daily `incremental`.** | **Memory risk — most decision-relevant gap** |
| **Q2** | Does Staging carry production-scale data? If not, where can a scale test run? | Phase 3 validity |
| **Q3** | Spike: can pydgraph set `set_nquads` **and** `del_nquads` on one mutation? (**[X]** both kwargs exist on `create_mutation`; simultaneous use unconfirmed.) | Phase 1 |
| **Q4** | Spike: can gRPC gzip be enabled via raw channel options under pydgraph? (**[X]** absent from its API; **[V]** always on in Go.) | Phase 1 |
| **Q5** | Which shadow-running mechanism (§3.3), and how many clean Staging cycles before Production? | **Phase 3 — decide before starting** |
| **Q6** | The pipeline defines a `dev` stage alongside `stg`/`prd-1` **[V]**, but only two environments are in use. Decommissioned, or usable as a throwaway target for Phase 2? | Phase 2 convenience |
| **Q7** | Who applies `schema.dql`, and does the deployed schema match the file? | Phase 3 |

---

## 7. Recommendation

**[R] Proceed with the chosen scope. It is the best risk-adjusted option available, and verification strengthened the case for it.**

Three points govern execution:

1. **Phase 0 is the gate, and it must be written in Go first.** This is the entire basis of the plan — tests written after the port would certify the port against itself. **[V]** The seam analysis is favourable: every RDF-building function takes plain structs, so the highest-value layer (L2 golden files) needs no database and no Dgraph.

2. **Resolve shadow-running before Phase 3.** **[V]** The ETL stamps its own `sync_time`, so running Go then Python against one Staging database compares nothing. Without snapshot/restore, the Staging mitigation yields a false green.

3. **Answer Q1 early.** With `initial` out of scope, **one** High risk remains that code review cannot settle: whether `incremental`'s materialised ID sets fit in memory under CPython at production volume. **[V]** The materialisation points are known (`events.go:177`, `links.go:113`); only the row counts are missing. If they are large, the fix is known — push chunking into SQL via keyset pagination — but it is a **design change to plan in Phase 2, not a translation**. Gathering those numbers requires no code and can happen today.

**[R] One scope note worth banking:** **[V]** because `initial` never imports `pkg/dgraph`, the retained Go binary ends up smaller and simpler than today's — a single-purpose Postgres→RDF exporter with no graph client, no mode dispatch, and no incremental logic. The two-language estate is therefore cheaper to carry than a 50/50 split would suggest.

---

**Sources for [X] claims:** [pydgraph repository](https://github.com/dgraph-io/pydgraph) (`README.md`, `pydgraph/client.py`, `pydgraph/client_stub.py` on `main`), [pydgraph on PyPI](https://pypi.org/project/pydgraph/), [Dgraph Python client docs](https://docs.dgraph.io/clients/python/), [Dgraph releases](https://www.dgraph.io/releases/); sibling repositories under `Repository-Azure/DATA-TRACEABILITY-SCRIPT/`.
