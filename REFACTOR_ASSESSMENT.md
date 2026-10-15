# REFACTOR_ASSESSMENT.md

Assessment: migrating the `incremental` ETL from Go to Python.

**Scope — confirmed.** Partial migration: `incremental` → Python; `initial` stays in Go, feature-frozen. Gated on a parity harness built first.

**Markers:** **[C]** confirmed decision by the owner · **[V]** verified in this repository (file/line) · **[X]** verified outside it · **[A]** assumption · **[R]** recommendation · **[?]** requires engineering verification before implementation.

This document is an assessment, not an execution plan. No code or configuration was modified.

---

## 1. Confirmed decisions

| # | Decision | Status |
|---|---|---|
| 1 | **Git baseline** — initial commit created | **[C]** **[V]** `ad372de`, 98 tracked files |
| 2 | **Test fixtures** — synthetic, all 7 scenarios; no real Staging data committed | **[C]** |
| 3 | **Runtime** — ECS Fargate | **[C]** |
| 4 | **Repositories** — two, separately owned | **[C]** Names in #13 |
| 5 | **Concurrency** — threads + `psycopg2` + synchronous `pydgraph` | **[C]** Matches team convention **[X]** |
| 6 | **Volume** — ~100k new rows/day; in-memory ID sets preferred | **[C]** **[R]** Assessed safe (§6) |
| 7 | **Shadow-running** — no DB dump/restore | **[C]** Differential moves offline to golden files (§4) |
| 8 | **Staging scale** — representative of Production; scale validation retained | **[C]** |
| 9 | **Self-gate removed** — no `traceability_log_daily` check in the Python ETL | **[C]** Not to be reintroduced |
| 10 | **Secrets** — AWS Secrets Manager, not bundled `.env` | **[C]** §7 |
| 11 | **Gate semantics + orchestration** — the original check only ensured the Traceability process for `entry_date = today − 3 days` had finished. Step Functions now orchestrates: the ETL runs **immediately after** the Traceability task and **must not proceed if Traceability fails** | **[C]** §5.2 |
| 12 | **Secrets Manager region** — `ap-southeast-3` | **[C]** |
| 13 | **Repository names** — `ETL-Dgraph-Initial` (Go), `ETL-Dgraph-Incremental` (Python) | **[C]** |
| 14 | **Canonical schema** — `ETL-Dgraph-Incremental` owns `schema.dql` | **[C]** **New.** Implications in §2.3 |
| 15 | **Repository rename** — `ETL-Dgraph-Initial` is a **rename** of the existing Go repository, preserving Git history including `ad372de` and golden-file provenance | **[C]** **New.** Implications in §2.4 |

---

## 2. Target architecture

```
┌─ ETL-Dgraph-Initial  (Go, renamed, feature-frozen) ──────────┐
│  main.go (initial path) · internal/app · pkg/database        │
│  internal/etl/common · internal/etl/initial                  │
│  Postgres → chunked gzip RDF → initial_imports/              │
│  NO Dgraph client [V]    History preserved: ad372de [C]      │
└──────────────────────────────────────────────────────────────┘
┌─ ETL-Dgraph-Incremental  (Python, new) ──────────────────────┐
│  config + Secrets Manager (ap-southeast-3)                   │
│  psycopg2 ThreadedConnectionPool · pydgraph sync             │
│  removal → events → links → sync_time (no matview refresh)   │
│  NO self-gate [C]        Owns canonical schema.dql [C]       │
│  ECS Fargate task, invoked by Step Functions                 │
└──────────────────────────────────────────────────────────────┘

Step Functions:   … → Traceability → ETL-Dgraph-Incremental → Matview refresh → …
                       └─ on failure: halt, do not proceed  [C]
```

**[V]** The split is clean: `pkg/dgraph` is imported only by `incremental/`, `runner.go`, and `main.go` — never by `internal/etl/initial` or `internal/etl/common`. `ETL-Dgraph-Initial` drops the Dgraph client entirely.

### 2.1 Cross-repository concerns

| Concern | **[R]** Recommendation |
|---|---|
| **Golden-file flow** | Generated in `ETL-Dgraph-Initial` during Phase 0; copied into `ETL-Dgraph-Incremental`, each file recording the source commit SHA. **[C]** History is preserved by the rename, so those SHAs remain resolvable (§2.4) |
| **Canonical schema** | **[C]** Owned by `ETL-Dgraph-Incremental` (§2.3) |
| **Shared contract** | schema predicates, `Event` type, `links` edge + 6 facet names, `unique_id` key. Document in both READMEs |
| **Runtime mutual exclusion** | `ETL-Dgraph-Initial` rebuilds the graph; `ETL-Dgraph-Incremental` mutates it. Never run concurrently — no code enforces this |

### 2.2 What is *not* shared — verified

**[V]** Blank-node labels never cross the boundary: the Go repo's are internal to one bulk load; the Python repo's are internal to a single mutation, mapped back via `blankToID`. **Byte-parity of `BlankNode` is not required** — only the property that two distinct `unique_id`s never collapse to one label within a batch.

**[A]** Literal formatting likewise need not match: differing-but-valid RDF text parses to the same stored value. Each side need only emit syntactically valid N-Quads.

### 2.3 Canonical schema ownership — implications for the Go ETL **[C]**

**The dependency is weaker than it appears.** **[V]** Neither binary ever loads `schema.dql` — there is no `SetSchema` call anywhere in `internal/`. **[V]** The file is nonetheless copied into all five images (root `Dockerfile:29` and each `deployment/dockerfiles/*:34`), where it is shipped but never read.

**[V]** The Go `initial` ETL's real coupling is *semantic*: it emits RDF using the predicate names the schema declares. The file's actual consumer is the **Dgraph bulk loader** (`dgraph bulk --schema`), whose invocation is not in this repository.

| # | Implication | **[R]** Recommendation |
|---|---|---|
| **I1** | The Go repo's copy becomes non-canonical, creating two copies that can drift | **Delete it from `ETL-Dgraph-Initial`** and have the bulk-load runbook fetch the canonical file from `ETL-Dgraph-Incremental`. Two schema copies drift silently, and the moment you discover it is during a bulk reload — i.e. bootstrap or disaster recovery, the worst time. This implies dropping the five `COPY schema.dql` lines **[V]**; documented here, not performed |
| **I2** | **Predicate drift, in the direction that matters.** `ETL-Dgraph-Initial` is feature-frozen, so its emitted predicate set is fixed. The canonical schema will evolve in the Python repo | Additive changes are safe — Initial simply never emits the new predicate. **Renames and removals are the hazard**: a future bulk reload would emit predicates the canonical schema no longer declares. **[R]** Run a predicate-drift check before any bulk reload, comparing the predicate set emitted by the Go code against the canonical schema |
| **I3** | **Schema application moves with ownership.** Applying the schema to a cluster is currently manual (V5) | The procedure belongs in the `ETL-Dgraph-Incremental` runbook. A new predicate must be pushed to the cluster **before** the Python ETL writes it |
| **I4** | A frozen binary shipping a file it never reads invites confusion about which copy is authoritative | Covered by I1 |

**[R] Net:** ownership by the Python repo is the right call — that is where predicates will change. The only real cost is the cross-repo fetch at bulk-reload time, which is a documented runbook step rather than an engineering problem.

### 2.4 Repository rename — implications **[C]**

**[C]** `ETL-Dgraph-Initial` is a rename of the existing repository, not a new one. Git history, baseline `ad372de`, and golden-file provenance are preserved.

| # | Implication | Status |
|---|---|---|
| **R1** | **No Go import rewrites needed.** **[V]** `go.mod` declares `module hara/traceability-dgraph-etl` — a bare module name, not a VCS URL. Renaming the repository does not affect any import path | **No action** |
| **R2** | **No hardcoded repository references.** **[V]** A search across `*.go`, `*.yml`, and `go.mod` found no repo name or URL outside third-party dependency paths | **No action** |
| **R3** | **Golden-file provenance holds.** Phase 0 golden files reference commit SHAs from this repository; preserved history keeps them resolvable | **Confirmed** — this is why the rename matters for §3 |
| **R4** | Local clones need `git remote set-url`; GitHub redirects the old URL, but updating explicitly is cleaner. Azure Pipelines' repository reference needs updating; branch triggers (`gar-dev`, `gar-stg`, `gar-prd-1`) are unaffected **[V]** | Routine |
| **R5** | ECR repository names and the Azure variable groups (`trace-dgraph-etl-*` **[V]**) need not change. Aligning them with the new names is cosmetic | Optional |

---

## 3. Parity harness (the gate)

**[R] The harness is written against the Go code first.** Tests written after the port would certify the port against itself. Golden files from Go runs are language-neutral and become the Python repo's acceptance criteria. **[C]** The rename preserves the history those files cite (§2.4 R3).

| Layer | Covers | Infrastructure | Verified seam | Effort |
|---|---|---|---|---|
| **L1 — Pure functions** | RDF literals, `buildLinkFacets`, `chunkIndices`, normalizers | **None** | package-level / struct-only funcs | **S** |
| **L2 — RDF golden files** | Set-or-delete semantics, facets, blank-node vs uid subject choice, batching | **None** — fake `dgraph.Adapter` captures the RDF strings | **[V]** `upsertEvents`, `upsertLinks`, `deleteExistingLinks` take plain structs | **M** |
| **L3 — SQL contract** | Which rows each CTE selects | **Real Postgres**, synthetic seed **[C]** | **[V]** `QueryRows` returns concrete `*sql.Rows` | **M** |
| **L4 — End-to-end** | Step ordering, `sync_time` stamping, graph state | Postgres + Dgraph | whole-binary | **L** |

**[R] L2 carries the most weight and needs no infrastructure** — it pins the invariants that corrupt data silently: set-or-delete per field, chunk-by-`event1_id` link replacement, facets only for non-empty values, existing-uid vs new-blank-node subject selection.

**[R] L3 must use a real database.** **[V]** Much of the business logic lives *in the SQL* — `eventIDsQuery`, `linkEvent1IDsQuery`, `removalQuery` are multi-CTE statements encoding the whole change-detection policy. **[R]** L3 ports for free: the SQL strings are identical in both languages.

**Fixture scenarios [C]:** new event · updated event · invalidated event · link added · link invalidated · event2-only change · event with no links.

**[R] Scenario 3 (invalidated event) is load-bearing** — §4.2 shows the removal path is what Staging cannot validate well without a restore.

---

## 4. Validation strategy

### 4.1 Differential moves offline

**[V]** The ETL mutates its own source of truth (`sync_time.go` stamps rows; Dgraph mutations are destructive). Without restore **[C]**, a Go-vs-Python differential on live data is not achievable.

| Stage | Method | Proves |
|---|---|---|
| **Offline (Phase 0–2)** | Golden files — Python reproduces Go's captured RDF payloads byte-for-byte | **Logic equivalence**. Stricter than an end-state diff: it compares exact mutation payloads |
| **Staging (Phase 3)** | **Verification harness** — after a run, re-query Postgres for the changeset and assert Dgraph matches: field values, links, facets, `sync_time` stamped | **Integration correctness** at real scale |

**[R] Build the verification harness as a reusable tool in `ETL-Dgraph-Incremental`.** It has standalone value as a Production monitoring check after every daily run.

### 4.2 Residual limitation — the removal path

**[R]** An optional deeper check without restore: capture the set `S` of IDs with `sync_time IS NULL`, run one implementation, capture graph state for `S`, then `UPDATE ... SET sync_time = NULL WHERE ... IN S` and run the other.

**Limitation — material.** The graph is not reset between runs. Event upserts and link replacement are idempotent and compare validly. **The removal path does not**: the first run deletes the invalid nodes, so the second finds no uids and does nothing. A removal bug would pass silently. Mitigated by L2 delete-payload coverage and fixture scenario 3.

### 4.3 Scale validation — in scope **[C]**

**[C]** Staging is representative of Production scale, so Phase 3 validates **throughput and memory**, not only correctness.

| Metric to capture per Staging run | Why |
|---|---|
| Cardinality of the two ID sets and `uidMap` | Validates the 2–3× amplification assumption empirically |
| Peak RSS | Validates the ~250 MB projection; sizes the Fargate task |
| Wall-clock duration, per phase | Sizes the Step Functions state timeout (§5.4) |
| Dgraph mutation count, retry/abort counts | Surfaces contention the golden files cannot show |

---

## 5. Orchestration — Step Functions **[C]**

**[C]** The Python ETL carries **no self-gate**. Ordering is the orchestrator's responsibility: the Fargate task runs immediately after the Traceability task, and the workflow **must not proceed to the Dgraph ETL if Traceability fails**.

### 5.1 What removing the gate takes with it — verified

| Effect | Evidence |
|---|---|
| **Unbounded Fargate billing eliminated** | **[V]** `traceability_check.go:59` is a `for {}` loop polling every 15 min, exiting only on context cancellation; **[V]** the context has no timeout |
| **tzdata dependency disappears** | **[V]** `time.LoadLocation("Asia/Jakarta")` appears **only** at `migrator.go:51`, solely to compute the gate's target date |
| **`traceability_log_daily` drops from required grants** | **[V]** Referenced only in `traceability_check.go` |
| **82 lines of polling logic not ported** | **[V]** `traceability_check.go` |

### 5.2 Ordering semantics — resolved **[C]**

**[C]** The original check only ensured the Traceability process for `entry_date = today − 3 days` had **finished running** — a completion check, not a data-lag dependency. **[V]** This matches the code: `targetDate = now(Jakarta) − 2 days` with SQL matching `entry_date + 1 day = targetDate`, testing only for `status = 'Running'`.

**[C]** Direct sequencing after the Traceability task, with a mandatory halt on failure, provides an equivalent-or-stronger guarantee: no Dgraph ETL run can overlap an in-flight Traceability run, and a failed Traceability day cannot silently let the chain continue.

**[R]** Missed days are self-healing and need no special handling: **[V]** the ETL selects on `sync_time IS NULL`, so rows skipped on a halted day are picked up by the next successful run.

### 5.3 Behavioural changes that remain

| # | Change | Assessment |
|---|---|---|
| **B1** | **No self-protection for out-of-band runs.** Previously an ad-hoc invocation would self-gate; now it would race the Traceability task | **[R]** Operational rule: invoke the Python ETL **only** via the workflow. Consider an IAM or ECS guard rather than relying on convention |
| **B2** | **Failure mode inverts.** Previously: wait indefinitely (silent stall, billed). Now: no wait — if ordering is misconfigured, it processes incomplete data (silent bad data) | **[C]** The halt-on-failure requirement addresses the main path. The residual depends on §5.5 (a) — without `.sync`, sequencing is void regardless |

### 5.4 Retry safety — verified, favourable

**[V] The run is idempotent and safe to retry.** All `sync_time` stamping happens in a **single final step** — `updateSyncTimes` runs after removal, events, and links complete:

- A failure **before** stamping leaves nothing stamped → the next run reprocesses the full changeset
- Each phase is independently idempotent: **[V]** removal deletes by uid (a re-run finds nothing and no-ops); events upsert by `unique_id`; links are delete-then-write per `event1_id`
- **[V]** Even a *partial* stamping failure is safe — `updateSyncTime` batches 200 rows per `UPDATE`; unstamped rows reprocess, already-stamped rows were correctly written
- **[C]** The matview refresh is **no longer part of the ETL** — `materialized_views.go` was deleted and a separate Step Functions state downstream now refreshes `mv_events`, `monitoring_cache_shipments` and `mv_shipment_list`. That state is idempotent and safe to retry on its own; an ETL retry no longer re-refreshes the views

**[R]** Configure a Step Functions `Retry` with exponential backoff. No application-level idempotency work is required. **[R]** Set `TimeoutSeconds` sized from the Staging durations captured in §4.3 — not guessed.

### 5.5 To verify before implementation **[?]**

| # | Item | Why it matters |
|---|---|---|
| **a** | **Integration pattern** — is the ECS task invoked with `.sync` (`arn:aws:states:::ecs:runTask.sync`)? | Without `.sync`, Step Functions fires and forgets: the next state starts immediately and **the ordering guarantee is void**. Highest priority |
| **b** | **Halt-on-failure enforced** in the state definition (`Catch`/`Next` transitions) | **[C]** is a requirement; confirm the definition implements it |
| **c** | **Retry policy** — attempts, backoff, error types | §5.4 shows retries are safe; the policy still needs defining |
| **d** | **Concurrent-execution guard** | Two concurrent runs would race on `sync_time` and link replacement |
| **e** | **Failure alarms/notifications** | The old gate's stall was at least visible in logs |

### 5.6 Cutover — decouple the two changes

**[R] Insert the *Go* binary into the workflow first, unchanged.** Its self-gate becomes a harmless no-op (it checks, finds `Done`, proceeds immediately). This validates the orchestration change — `.sync` behaviour, halt-on-failure, retries, timeouts — **independently of the language port**. Then swap the task definition to the Python image.

---

## 6. Memory assessment at ~100k rows/day **[C]**

**[R] Verdict: in-memory ID sets are safe. Keep them. Keyset pagination is not required.**

**[V]** Three structures persist for the run: the event ID set (`events.go:177`), the event1 ID set (`links.go:113`), and `uidMap` — which grows **monotonically**, merged per chunk and passed from `syncEvents` into `syncLinks`.

**[A]** 100k new rows ≠ 100k IDs. **[V]** `eventIDsQuery` selects endpoints of links with `sync_time IS NULL`, pulling in already-synced events; `linkEvent1IDsQuery` adds `event1` of any link whose `event2` is unsynced. **[A]** Assume 2–3× → ~200–300k IDs.

| Structure (at 300k IDs, ~36-char keys) | Estimate |
|---|---|
| ID strings (~85 B each) | ~26 MB |
| Set table overhead | ~16 MB |
| `uidMap` dict + uid value strings | ~45–70 MB |
| **Structures total** | **~90–110 MB** |
| Interpreter + grpc + psycopg2 baseline **[A]** | ~80–120 MB |
| **Projected peak RSS** | **~250 MB** |

**[R]** Comfortable at any realistic Fargate sizing (≥1 GB). At 10× (~3M IDs) this reaches ~1 GB of structures and would need 2–4 GB — that is the point to revisit.

### 6.1 Guardrail

1. **Measure before Phase 2** — `SELECT COUNT(*)` over `eventIDsQuery` and `linkEvent1IDsQuery` against Production. Converts the amplification **[A]** into a verified figure in minutes, no code
2. **Capture cardinality and peak RSS every Staging run** (§4.3) — **[C]** Staging is production-scale, so these numbers are meaningful
3. **Revisit at ~1M IDs** — the fix (keyset pagination in SQL) is known but is a design change; trigger it on data, not in an incident

**[R]** The `StringInterner` (**[V]** 32-shard, built for `initial`'s scale) is **not needed** in the Python repo.

---

## 7. Secrets management **[C]**

**[C]** AWS Secrets Manager in **`ap-southeast-3`**, replacing the bundled `.env`. **[V]** Today the Go pipeline writes a `.env` from Azure variable groups (`build.yml`) and the deployment Dockerfiles `COPY .env` into the image — `ETL-Dgraph-Incremental` must not inherit this.

**[V]** The Go repo's `config/app.yaml` defaults `region: ap-southeast-1`, but **[V]** `pkg/aws` is imported by no ETL path — dead config, no reconciliation needed.

### 7.1 IAM

| Pattern | Role needing permission | Notes |
|---|---|---|
| **Runtime fetch via `boto3`** *(recommended — team convention **[X]**)* | **ECS task role** | App fetches once at startup; secret names stay in config |
| **ECS task-definition injection** (`secrets` / `valueFrom`) | **ECS task execution role** | No `boto3` dependency; simpler, less flexible |

- `secretsmanager:GetSecretValue` — **[R]** scoped to the exact secret ARN per environment (`arn:aws:secretsmanager:ap-southeast-3:<account>:secret:<name>`), never `*`
- `kms:Decrypt` — only if the secret uses a customer-managed KMS key

**[R]** Separate secrets per environment with separate ARNs, so the Staging role cannot read Production credentials.

### 7.2 Deployment considerations

| Item | Action |
|---|---|
| **Pipeline** | Drop the `file-creator` `.env` step **[V]**. With credentials in Secrets Manager, the Azure variable groups shrink to build/deploy metadata only (AWS account, region, ECR repo) |
| **Dockerfile** | Must **not** `COPY .env` **[V]** |
| **Config split** | Secrets (`DB_CLEAN_*`, `DGRAPH_*` credentials) → Secrets Manager. Non-secret tunables stay in `app.yaml`/env vars |
| **VPC endpoint** | **[R]** If the task runs in private subnets without NAT, an interface endpoint for `com.amazonaws.ap-southeast-3.secretsmanager` is **required** — a common Fargate failure. **[?]** Confirm networking |
| **Fetch pattern** | Once at startup, not per connection |
| **Rotation / audit** | Daily batch picks up rotated values at next start; `GetSecretValue` is logged to CloudTrail |

---

## 8. Component matrix

Effort: **S** ≤ 3 days · **M** ~1–2 weeks · **L** ~3–6 weeks. **[A]** One experienced developer, excluding validation.

| Component | Verdict | Effort | Notes |
|---|---|---|---|
| `internal/etl/initial` (778 lines) | **Stays in `ETL-Dgraph-Initial` — frozen** | — | Rare bootstrap; worst CPython profile |
| `internal/etl/incremental` (1,893 lines) | **Port to `ETL-Dgraph-Incremental`** | L | Minus the gate (§5.1) |
| `internal/etl/incremental/traceability_check.go` (82 lines) | **Not ported** **[C]** | — | Replaced by Step Functions sequencing |
| `schema.dql` | **Moves to `ETL-Dgraph-Incremental`** **[C]** | S | §2.3. Implies dropping five `COPY schema.dql` lines from the Go images **[V]** |
| `pkg/dgraph` | **Delete from the Go repo** → Python owns | M | **[V]** Imported only by incremental/runner/main. Must re-implement the retry classifier (**[V]** `isRetryableError`, ~18 patterns) and resolve the gzip gap (V3) |
| `pkg/database` | **Keep in Go repo**; reimplement in Python | S | **[C]** `psycopg2`. **[R]** `ThreadedConnectionPool`; named (server-side) cursors for the large ID queries |
| Concurrency (`fanOutBatches`) | **Reimplement in Python** | S | **[C]** Threads. **[V]** Config defaults are `concurrency: 4` with batches of 50–100 — a small `ThreadPoolExecutor` suffices. **[R]** One pydgraph `Txn` per thread |
| `internal/app` config | **Keep**; reimplement in Python | S | Plus Secrets Manager integration (§7) |
| `main.go`, `runner.go`, `mode.go` | **Simplify in the Go repo** | S | Drop the incremental branch and two-mode dispatch |
| `common.StringInterner` | **Not needed in Python** | — | §6.1 |
| `pkg/util` | **Keep subset in Go repo**; drop for Python | S | **[V]** Still used by the Go binary |
| `pkg/aws`, `pkg/httpx`, `pkg/validator`, `pkg/errors`, most of `pkg/logger` | **Drop from both** | S | **[V]** Unused by any ETL path |
| **Verification harness** | **New in `ETL-Dgraph-Incremental`** | M | §4.1; reusable as Production monitoring |
| `deployment/` | **New pipeline for the Python repo** | M | Plus ECR repo, IAM roles, Step Functions state |

---

## 9. Risk register

| Risk | Severity | Status |
|---|---|---|
| **`initial` mode performance** | — | **Eliminated** by scope |
| **Staging ≠ Production scale** | — | **Eliminated** **[C]** |
| **Unbounded gate billing** | — | **Eliminated** **[C]** |
| **Ordering / data-lag semantics** | — | **Eliminated** **[C]** — §5.2 |
| **Golden-file provenance loss** | — | **Eliminated** **[C]** — rename preserves history (§2.4 R3) |
| **Memory blow-up** | **Low** | §6; empirically validated in Staging |
| **`BlankNode` divergence** | **Low** | **[V]** Labels are load-/mutation-local |
| **No test oracle** | **Low** | Phase 0, written against Go first |
| **Ordering void if `.sync` absent** | **Medium** | **[?]** §5.5 a — the single highest open item |
| **Schema predicate drift at bulk reload** | **Medium** | **New** (§2.3 I2). Mitigated by a pre-reload drift check |
| **Out-of-band invocation races Traceability** | **Medium** | §5.3 B1 |
| **Removal path under-validated in Staging** | **Medium** | §4.2; mitigated by L2 coverage + fixture scenario 3 |
| **Silent graph corruption from a porting error** | **Medium** | Golden files + verification harness |
| **Two-repo estate** | **Medium** | Accepted; mitigated by freezing the Go repo and the narrow contract |
| **Fixture realism** | **Medium** | Synthetic fixtures cover only what is seeded |
| **No in-house pydgraph experience** | **Medium** | **[X]** No Python↔Dgraph integration exists in this workspace |

### Assumptions

- **[A]** 2–3× amplification between "new rows" and "IDs in the set" — measurable (§6.1), currently unverified
- **[A]** ~36-character `unique_id`; memory estimates scale linearly with length
- **[A]** The Step Functions workflow runs daily
- **[A]** One experienced developer; estimates exclude review and change-management overhead
- **[A]** `initial` runs rarely enough that a frozen Go binary is acceptable indefinitely

---

## 10. Remaining uncertainties

**All owner decisions are confirmed. No genuine blockers remain.** What follows is engineering verification, carrying no open decision.

| # | Item | Priority |
|---|---|---|
| **V1** | Step Functions `.sync` integration pattern (§5.5 a) — **ordering guarantee is void if absent** | **Highest** |
| **V2** | Halt-on-failure enforced in the state definition; retry policy; timeout; concurrent-execution guard; failure alarms (§5.5 b–e) | High |
| **V3** | Spike: `set_nquads` + `del_nquads` on one pydgraph mutation — **[X]** both kwargs exist; simultaneous use unconfirmed | Medium |
| **V4** | Spike: gRPC gzip via raw channel options under pydgraph — **[X]** absent from its API; **[V]** always on in Go | Medium |
| **V5** | The schema-application procedure (manual today) — now owned by `ETL-Dgraph-Incremental` (§2.3 I3), but the process itself is undocumented, as is whether the deployed schema matches the file | Medium |
| **V6** | Fargate networking — VPC endpoint for `com.amazonaws.ap-southeast-3.secretsmanager` if private subnets without NAT (§7.2) | Medium |
| **V7** | Bulk-loader invocation (`dgraph bulk --schema`) is not in either repository; it becomes a cross-repo runbook step under §2.3 I1 | Low |

**[A]** Carried assumption worth retesting at Phase 2: the 2–3× ID amplification (§6.1), cheaply convertible to a verified figure.

---

## 11. Recommendation

**[R] The assessment is complete. All fifteen decisions are confirmed; proceed to planning.**

The two final confirmations were both low-risk and one was actively helpful:

- **Canonical schema in `ETL-Dgraph-Incremental`** (#14) is the right owner — that is where predicates will change. **[V]** The coupling to the Go ETL is weaker than it looks: neither binary ever reads `schema.dql`; the Go side merely *emits predicates the schema declares*, and the file's real consumer is the bulk loader. The one new risk is **predicate drift** (§2.3 I2) — additive schema changes are safe, but a rename or removal would leave the frozen Go exporter emitting predicates the canonical schema no longer declares. A pre-reload drift check closes it.
- **Rename rather than new repository** (#15) preserves exactly what the parity harness depends on. **[V]** It also costs nothing technically: `go.mod` declares a bare module name (`hara/traceability-dgraph-etl`), not a VCS URL, so no import rewrites are needed, and no repository name appears anywhere in the code or pipelines.

**[R] Two things to carry into implementation, unchanged from the prior revision:**

1. **V1 is the single highest open item.** If the ECS task is invoked without `.sync`, Step Functions does not wait for completion, the next state starts immediately, and the ordering guarantee that replaced the gate does not exist. Everything in §5 rests on it.
2. **Sequence the cutover (§5.6)** — move the Go binary into the workflow first, where its gate is a harmless no-op, and validate orchestration independently of the port.

**[R] And one cheap step before Phase 2:** run `SELECT COUNT(*)` over `eventIDsQuery` and `linkEvent1IDsQuery` against Production (§6.1). It is the last carried assumption that can be converted to a fact without writing code.

---

**Sources for [X] claims:** [pydgraph repository](https://github.com/dgraph-io/pydgraph) (`README.md`, `pydgraph/client.py`, `client_stub.py` on `main`), [pydgraph on PyPI](https://pypi.org/project/pydgraph/), [Dgraph Python client docs](https://docs.dgraph.io/clients/python/); sibling repositories under `Repository-Azure/DATA-TRACEABILITY-SCRIPT/`.
