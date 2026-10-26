# REFACTOR_PLAN.md

Phased execution plan: migrating the incremental ETL from Go to Python.

**Source of truth:** `REFACTOR_ASSESSMENT.md`. This plan translates its 15 confirmed decisions, architecture, and risk register into executable phases. It does not introduce new architectural decisions; anything unresolved in the assessment appears here as an **explicit investigation task**, never as an assumption.

**Markers (carried from the assessment):** **[C]** confirmed decision · **[V]** verified in the Go repository · **[X]** verified externally · **[A]** assumption · **[R]** recommendation · **[?]** requires investigation · **[O]** owner-owned (outside this plan's execution scope; the plan consumes the result) · **⏳** pending owner input · **⏸** deferred to a later, named point (not dropped).

**Repositories** **[C] (H1, PD-21)** — three local directories side by side; the new two are **not** renames of the working repository, which stays where it is.

| Repository | Local path | Origin | Role |
|---|---|---|---|
| **`Dgraph`** (current) | `D:\Work\Dattabot X HARA\Repository-GitHub\Dgraph` | existing | The working repository and the source for `ETL-Dgraph-Initial`. Baseline **`b487294`** |
| **`ETL-Dgraph-Initial`** | `D:\Work\Dattabot X HARA\Repository-GitHub\ETL-Dgraph-Initial` | **cloned from `Dgraph`, history preserved** | Go, feature-frozen |
| **`ETL-Dgraph-Incremental`** | `D:\Work\Dattabot X HARA\Repository-GitHub\ETL-Dgraph-Incremental` | **new, no history to preserve** | Python; owns canonical `schema.dql` |

**[V]** Neither target directory exists yet; both are created in P0-1 and P3-1.

### Plan-level decisions

Distinct from the assessment's 15 architectural decisions; these govern execution gates and ownership only.

| # | Decision | Detail |
|---|---|---|
| **PD-1** | **P6 exit threshold** — ≥5 consecutive **clean daily cycles**, with objective criteria and a reset rule | **[C]** §9.1–9.2 |
| **PD-2** | **Production soak** — minimum **14 calendar days** retaining Go for rollback; decommission only when all four gate conditions hold | **[C]** §10.2–10.4 |
| **PD-3** | **Environment progression** — **Local → Staging → Production**, strictly in order. Local uses **synthetic data only**; data copied from Staging or Production is never used locally. An environment is entered only when the previous one's exit criteria are met | **[C]** §2 |
| **PD-4** | **Ownership boundary** — this plan owns the **container and ECS task readiness** (image, configuration, secrets access, logging, error handling, exit status). **Workflow placement and Step Functions orchestration integration are owner-owned** **[O]**; the plan consumes their outcome as a fact, and does not investigate or decide them | **[C]** §2.4, §5 |
| **PD-5** | **Image tagging** — the Python image is published to the **existing** ECR repository `gar-stg-trace-ecr-dgraph-etl` under the dedicated tag **`dgraph-incremental`**. The existing Go image and its tag are **preserved as the rollback artefact and never overwritten** | **[C]** §8.4 |
| **PD-6** | **Task-definition revision** — deploy the **latest applicable revision** of the `GAR-Stg-Trace-Task-ETL-Dgraph` family. The revision must be **identified and verified before deployment** by the §8.6 procedure. **No Step Functions or workflow configuration is modified** | **[C]** §8.6 |
| **PD-7** | **Task-definition registration is manual** for now — performed by the §8.6 runbook. **No pipeline or IaC automation** is introduced for it | **[C]** §8.6 |
| **PD-8** | **Secrets** — `stg/etl-dgraph/creds` holds **both the Postgres connection coordinates and the credentials**; none of them appear in the ECS task definition. PostgreSQL **`sslmode=disable`** — TLS is not in use | **[C]** §8.5 |
| **PD-9** | **Task-definition reference** — the **family name** `GAR-Stg-Trace-Task-ETL-Dgraph` is the intended reference, resolving to the latest `ACTIVE` revision. Step Functions and workflow integration are **not inspected, modified, or decided** here | **[C]** §8.6, PD-4 |
| **PD-10** | **CI/CD is deferred until Staging is complete** — design and implementation both. **No pipeline is a prerequisite for any Staging task**: the image is built and pushed by hand, and task definitions are registered by hand (PD-7) | **[C]** §15 |
| **PD-15** | **Watermark semantics — revised.** **`sync_time = NULL` marks a row as pending for the Incremental ETL.** Two consequences: **(a)** every **new or updated** `events_bc` / `links_bc` row must have `sync_time = NULL` so the Incremental ETL detects it; **(b)** rows already loaded by the Initial ETL must **not** be `NULL` once that load succeeds, so Day 1–Day 5 process **only post-Initial changes, never the full dataset**. **Supersedes the earlier wording** ("`NULL` for all rows Initial processed"), which is incompatible with (b) — see §9.0.1 | **[C]** §9.0.1 |
| **PD-18** | **Post-initialization stamping** — after the Initial ETL succeeds **and its output is verified**, set `sync_time = NOW()` for the processed `events_bc` / `links_bc` rows whose `sync_time` is still `NULL`. Ordering and scope in §9.0.1 | **[C]** §9.0.1 |
| **PD-19** | **Loader — `dgraph bulk`.** Selected on repository evidence and the provisioned cluster, not preference (§9.0.4). `dgraph live` is **not** adopted: it would be a different approach, and nothing in the evidence justifies one | **[C]** §9.0.4 |
| **PD-20** | **Watermark re-nulling is external** — returning `sync_time` to `NULL` for new or updated source records happens **outside the Dgraph ETL's scope**. This plan neither investigates, documents, nor requires details of that process; it only consumes the result | **[C]** §9.0.1 |
| **PD-16** | **Initial and Incremental never run concurrently** — an **operational assumption**, not an orchestration mechanism. Initial runs **once** for the initial load, or is **rerun when a major change requires refreshing all data in Dgraph** | **[C]** §9.0.2 |
| **PD-17** | **Expected changesets are owner-supplied** — before each Incremental run the owner identifies and records which source records were inserted or updated. The harness **consumes** that record as its oracle; this plan does **not** determine or generate it | **[C]** §9.0.3 |
| **PD-12** | **Staging data lifecycle** — Production data is updated **daily**; Staging data is updated **less frequently**, by design. Staging remains **representative of Production in content and per-run volume**, but its cadence is not. Validation cycles are therefore **owner-seeded, not naturally occurring** | **[C]** §9.0 |
| **PD-13** | **Staging validation sequence** — **Initial ETL once** against the Staging dataset to bootstrap the graph, then **Incremental ETL on Day 1 … Day 5**. Before each daily run the owner **manually inserts and/or updates source data**; the volume varies by day and stays **below 100,000 records** | **[C]** §9.0 |
| **PD-14** | **Step Functions orchestration is out of scope** — this plan does **not** investigate, request, or modify the state machine, and **no Step Functions behaviour is a prerequisite, dependency, or blocker** for any task, gate, or acceptance criterion here. Ordering and upstream-failure handling are owned entirely by the owner | **[C]** §5, PD-4 |
| **PD-21** | **Repository locations — confirmed.** Both new repositories are **separate local directories alongside** the current one, at the paths tabulated above. `ETL-Dgraph-Initial` is created by **cloning `Dgraph` locally so Git history is preserved**, then pushed to its remote once that remote exists (H1 unchanged). `ETL-Dgraph-Incremental` is a new repository with no history to carry | **[C]** §3.0 |
| **PD-11** | **Target branch model — `stg` and `prd` only.** **Dev is removed**; it is not an actual environment. The two environments live in **separate AWS accounts** and share **one tag naming convention**, which is safe because an ECR repository is scoped to one account and region | **[C]** §15.2, §15.4 |
| **PD-22** | **Memory — the ID backlog is never held in memory.** *(Owner, 2026-10-18.)* The ID queries run **unchanged** and are **streamed** (server-side cursor, P3-7) into a **disk-backed run ledger**; every later step works on chunks read back from it, so resident state is O(chunk) whatever the backlog. Keyset pagination is **not** adopted. **Supersedes** assessment decision 6 ("in-memory ID sets preferred") and **resolves** the §3 stop-and-reassess trigger. Design, evidence and limits: §7.0 | **[C]** §7.0, P4-15 … P4-18 |
| **PD-23** | **Go parity is the specification.** P4 **preserves** three existing Go behaviours exactly — (1) an event with no valid link is neither synced nor stamped; (2) a link whose endpoint has no uid is dropped but **still stamped**; (3) a malformed uid-lookup response reads as "not found". They are recorded as **post-refactor follow-ups** FU-1 … FU-3 (§14.3), **not** fixed in this refactor | **[C]** §14.3 |
| **PD-24** | **The synthetic Postgres fixture stays owned by `ETL-Dgraph-Initial`.** The Python repository **invokes the Go repository's own script in place** (`scripts/fixture.sh`) and never copies it; `tests/fixture.lock` pins its hashes so the fixture and the golden corpus cannot drift apart silently | **[C]** P4-10, P4-11 |
| **PD-25** | **Real Secrets Manager validation stays scheduled for P5-21.** Nothing before it reads a real secret: P3 verified the loader against a stubbed boto3 client in the pinned region, and P4 runs with `SECRETS_SOURCE=env` against synthetic data | **[C]** P3 acceptance, P5-21 |
| **PD-26** | **`sync_time` stamping stays Go-compatible: per batch, not all-or-nothing.** *(Owner, 2026-10-19.)* Each 200-row `UPDATE` commits on its own and a failed `events_bc` batch does not stop `links_bc` stamping, exactly as Go. A failure **during** stamping may therefore leave partial stamps; a re-run is safe (the unstamped rest is reprocessed by idempotent upserts). The P4-6 guarantee is "nothing is stamped until every graph phase has succeeded", not atomicity. **Do not change this behaviour** | **[C]** P4-6 |
| **PD-27** | **The LRU in front of the ledger's `uids` table is mandatory** as specified in P4-15 / §7.0 step 3. *(Owner, 2026-10-19.)* | **[C]** P4-15 |
| **PD-28** | **A stale ledger directory after `SIGKILL` is acceptable.** *(Owner, 2026-10-19.)* The target storage is ephemeral; no extra cleanup mechanism is added. Normal exit and exceptions still delete the ledger | **[C]** P4-18, §7.0 |

---

## 1. Phase overview

| Phase | Name | Env | Repo | Gates | **[A]** Effort |
|---|---|---|---|---|---|
| **P0** | Investigations & Baseline | — | both | everything | 1–2 wks |
| **P1** | Parity Harness | Local | Initial (Go) | **all Python work** | 2–4 wks |
| ~~P2~~ | ~~Orchestration Validation~~ — **[C] withdrawn (PD-14)**, owner-owned, gates nothing | — | — | — | — |
| **P3** | Python Foundation | Local | Incremental | P4 | 1–2 wks |
| **P4** | Port Incremental Logic + Local E2E | Local | Incremental | P5 | 3–6 wks |
| **P5** | Verification Harness, Container & Deployment Infra | Local → Staging | Incremental + AWS | P6 | 2–3 wks |
| **P6** | Staging Validation | Staging | Incremental | P7 | ≥5 clean cycles **[C]** |
| **P7** | Production Cutover & Decommission | Production | both | — | ≥14-day soak **[C]** |

### Dependency graph

```
P0 ──> P1 ──> P3 ──> P4 ──> P5 ──> P6 ──> P7

Local ───────────────────────> Staging ──────> Production
 P1 · P3 · P4 · P5.1–5.3         P5.4 · P6            P7

Step Functions orchestration — owner-owned, out of scope (PD-14); gates nothing above
```

**Critical path:** P0 → P1 → P3 → P4 → P5 → P6 → P7. **[A]** ~3–4 months to cutover, plus the 14-day soak before decommission. Validation, not coding, is the long pole.

**Two things sit outside this phase model and gate nothing in it:** **[C]** PD-10 — CI/CD, deferred until Staging completes (§15); and **[C]** PD-14 — Step Functions orchestration, owner-owned (§5).

---

## 2. Environment progression **[C] (PD-3)**

Three environments, entered strictly in order. Each has its own objective, prerequisites, task set, and exit criteria. Nothing in a later environment starts before the previous one's exit criteria are recorded as met.

### 2.1 Local

**Objective.** Prove logic equivalence and end-to-end correctness against a fully controlled stack — Postgres *and* Dgraph both running locally — before any AWS resource is touched.

**Data rule [C].** **Synthetic/dummy data only.** No dump, extract, sample, or anonymised copy of Staging or Production data is used locally, at any point, for any reason. The fixture built in P1-6/P1-7 (seven scenarios, synthetic) is the *only* local data source and is shared verbatim by the Go and Python sides.

**Prerequisites.**
- ☑ P0-0 (baseline commit `b487294`) · ☑ P0-1 (create `ETL-Dgraph-Initial` at the PD-21 path, §3.0)
- ☑ **P0-6** (schema-application procedure, INV-5) — needed by **P4-11**, **not** by P1; P1 is Postgres-only and can start before it lands
- **No remote repository is required** to begin **[C]** §3.0
- **Not a prerequisite: INV-7.** **[C]** The `dgraph bulk` procedure is **deferred** (P0-8) and blocks nothing Local — P4-10's local stack applies `schema.dql` to a local alpha by the INV-5 procedure and seeds Postgres directly, with no bulk load anywhere in the Local path
- Local Dgraph version pinned to the deployed cluster version (INV-9)

**Tasks.** P1 in full · P3 in full · P4 in full, including the local end-to-end block (§7.2) · P5.1–P5.3 (harness, image, local container rehearsal).

**Exit criteria.**
- ☑ P1, P3, P4 acceptance criteria all met **[X]** recorded in §4, §6 and §7. P3's live Secrets Manager resolution is deferred to P5-21 by PD-25 and is not a P3 criterion
- ☑ All **7 scenarios** pass end-to-end against local Postgres + local Dgraph, from the Python ETL **[X]** Stage 4, §7.2 (P4-12): S1 … S7 and ALL, asserted after each of the four phases
- ☑ **Every P1 golden file reproduced byte-for-byte** by the Python port **[X]** §7 acceptance, over each golden's canonical form
- ☑ The **built container image** runs the full pipeline against the local stack and **exits 0**; a forced failure exits **non-zero** **[X]** 2026-10-19, decision O-6 (b): the **exact deployment image** exits 0 against the local stack (credentials from a local fake Secrets Manager; the harness from the same image passes), and forced failures exit non-zero: the credential-path ones on the deployment image, kill / alpha stopped / Postgres unreachable through the `rehearsal` image, which differs from it in one file (§8.2)
- ☑ The verification harness passes against a local run **and** fails its negative test (injected discrepancy) **[X]** Stage 5, §8.1 (P5-6): passes on real runs of all 8 scenarios and on Go's own end state; fails on each of four injections and on eight corruptions of Go's end state. It also passes when run from both container images (§8.2)
- ☑ No real-data artefact exists in the repository or the local stack — verified by the **test-suite guards** in P4-14 **[C]** (D4), which run offline and need no credentials **[X]** Stage 4 (P4-14); the guards, and a scan of 107 non-golden text files, are clean after Stages 5 and 6
- ☑ Peak RSS measured locally and recorded as the first comparison point against the §6 projection **[X]** Stage 4: **50 MB** on the fixture, **64.9 MB** at ~100k synthetic records, against ~250 MB projected

### 2.2 Staging

**Objective.** Validate correctness, Go↔Python parity, performance, memory, and operational behaviour at real scale. **[C]** Staging is representative of Production **in content and per-run volume** (assessment decision 8), so this is where scale validation happens — not Production. **[C]** PD-12: its **update cadence differs by design**, which is why the five cycles are **owner-seeded** rather than naturally occurring.

**Prerequisites.**
- Local exit criteria met
- Staging deployment configuration verified (§8.4) and a new task-definition revision registered
- **INV-7 answered — mandatory, and this is where it falls due.** **[C]** Deferred through Local (P0-8), it must be resolved **before P6-0b**, the Staging Initial-ETL bootstrap. Take it up when Local validation is complete and Staging implementation is about to begin

**Two things are explicitly not prerequisites.** **[C]** PD-10 — CI/CD: Staging needs an image in ECR and a registered revision, and a local `docker build` / `docker push` plus §8.6 produce both. **[C]** PD-14 — Step Functions: no orchestration behaviour gates anything here.

**Tasks.** P5.4 (deploy infra against the provided Staging resources) · P6 in full, beginning with the §9.0 bootstrap.

**Exit criteria.**
- ☐ The **Initial ETL bootstrap** (§9.0) completed, and the **post-initialization watermark established** — rows loaded by Initial are stamped, so Day 1 sees only post-Initial changes (**[C]** PD-15, §9.0.1)
- ☐ **5 consecutive clean cycles, Day 1 … Day 5** per PD-1 and PD-13 (§9.1 dimensions A1–A3, B1–B4, C1–C5)
- ☐ Go↔Python parity: the verification harness reports **zero** discrepancies across the whole count
- ☐ Performance: per-phase and total durations recorded for every run, and within the §9.1 **C2** band once the first clean cycle sets it
- ☐ Memory: peak RSS within the §6 envelope, no OOM; task right-sized from measurement (P6-7)
- ☐ Operational: retry demonstrated (P6-5), logs/alarms observed working, exit statuses propagate correctly
- ☐ Zero open defects classified Critical or Major (§10.3)

### 2.3 Production

**Objective.** Cut over, soak, decommission — nothing is proven here for the first time.

**Prerequisites.** Staging exit criteria met and recorded.

**Tasks.** P7 in full.

**Exit criteria.** G1–G4 (§10.4).

### 2.4 Ownership boundary **[C] (PD-4)**

| Area | Owner |
|---|---|
| Python ETL code, container image, entrypoint, exit statuses | **This plan** |
| ECS task definition content: image, resources, roles, log configuration, env/secret wiring | **This plan** (§8.3) |
| Secrets Manager access from the running task, failure handling, logging | **This plan** |
| **Step Functions orchestration — entirely**: which workflow triggers the task and where, `.sync`, `Catch`/`Next`, retry, timeout, concurrency guard, alarms, and **preventing the Incremental ETL from running when Traceability fails** | **[C] Out of scope (PD-14)** — owner-owned; not investigated, requested, or modified here, and never a blocker |
| **Materialized-view refresh — entirely** | **[C] Out of scope (D2)** — see §5

**[C]** PD-14 removes the last dependency this plan had on owner-owned orchestration: **no phase, gate, or acceptance criterion here waits on a Step Functions outcome.** What the plan still owes the owner is the ETL-side contract in §5 — exit-code semantics, retry safety, and the concurrency constraint — so the state machine can be configured without reading the ETL source.

---

## 3. P0 — Investigations & Baseline

**Objective.** Convert every open `[?]` in the assessment into a verified fact, and establish the repository baseline. No implementation.

**Dependencies.** None.

### 3.0 Repository setup — steps and what they do not require **[C] (PD-21)**

**[V]** Neither directory exists. Both are created here; nothing below needs a remote.

**`ETL-Dgraph-Initial` — P0-1.** History preservation is the whole point (P1-9 cites commit SHAs), so the repository is **cloned**, not copied:

```bash
git clone "D:/Work/Dattabot X HARA/Repository-GitHub/Dgraph" \
          "D:/Work/Dattabot X HARA/Repository-GitHub/ETL-Dgraph-Initial"
```

A clone carries every commit, `b487294` included. A `git init` plus a file copy does not, and would make P1-9's SHA citations unresolvable.

**[R] Immediately afterwards, repoint `origin`.** The clone's `origin` will be the local `Dgraph` path, and `Dgraph` itself has an `origin` **[V]** pointing at the working mirror. Leaving either in place risks a push landing in the wrong repository. Remove or rename the inherited `origin` as part of P0-1, and add the real remote only when PD-21's remote details are known.

**`ETL-Dgraph-Incremental` — P3-1.** A **new** repository at the PD-21 path; **[C]** no history to preserve, so `git init` is correct here. It receives `schema.dql` (P3-2) and the golden-file corpus exported by P1-9.

**Remote repositories — not required to begin.** **[?]** The hosting platform, organisation/account, and remote URLs are **not known and are not assumed**. They are **not needed for P0-1, P1, P3, P4, or P5.1–P5.3**: all of that work — cloning, committing, `go test`, golden files, SHA citations, the local stack, and the local container run — happens in local repositories. The remote becomes necessary at the first of these:

| Trigger | Why |
|---|---|
| Work must survive loss of this machine | A local-only repository has no backup |
| Anyone other than the local developer must resolve a golden-file SHA | P1-9's provenance claim is only as verifiable as the repository it lives in |
| CI/CD implementation begins | **[C]** PD-10 — deferred until after Staging |

**[R]** Push both repositories as soon as the remote is known, rather than waiting for a trigger; the cost is one command and the exposure it removes is total loss of the parity harness.

**[?] One cheap thing to settle early — which copy of this plan is canonical.** Cloning `Dgraph` copies `REFACTOR_PLAN.md`, `REFACTOR_ASSESSMENT.md`, `BASE_KNOWLEDGE.md` and `CLAUDE.md` into `ETL-Dgraph-Initial`. Two editable copies of a living document drift. **[R]** Keep `Dgraph` as the home of the planning documents and delete or freeze the copies in `ETL-Dgraph-Initial`, or pick the opposite — but pick. Non-blocking; cheaper decided now than discovered later.

### Tasks

| ID | Repo / Area | Task | Source |
|---|---|---|---|
| ☑ P0-0 | Repo | **Baseline commit — done [C].** **[V]** `b487294e04cdb6fea5e49224364830e9762d2f58` (short `b487294`), working tree clean, parent `ad372de`. It contains the MV-refresh removal (`materialized_views.go` deleted, the `migrator.go` call removed) **[V]**, so the committed code is the behaviour P1 encodes. **`b487294` is the P1 baseline**, superseding `ad372de` |
| ☑ P0-1 | Initial | **Create `ETL-Dgraph-Initial`** at the PD-21 path, per the §3.0 setup steps. **[C]** H1 — not a rename; history preserved; the **original upstream repository remains active** and is no longer the source. **[V]** No import rewrites needed — `go.mod` declares a bare module name. **[X] Done 2026-10-16:** clone HEAD `b487294`, ancestors resolve, inherited `origin` removed, no new remote added (report §2) | §3.0, H1 |
| ~~P0-2~~ | — | ~~INV-1 — `.sync` integration pattern~~ — **[C] withdrawn (PD-14):** Step Functions is out of scope and gates nothing here | — |
| ~~P0-3~~ | — | ~~INV-2 — halt-on-failure, retry, timeout, concurrency guard, alarms~~ — **[C] withdrawn (PD-14)** | — |
| ☑ P0-4 | Python | **INV-3:** Spike — can pydgraph set `set_nquads` **and** `del_nquads` on one mutation? **[X]** Both kwargs exist. **Answered YES (2026-10-16, deployed v25.3.1):** one mutation carries both and applies both atomically; ~~same-predicate set+del in one mutation → **delete wins**~~ **[X] corrected 2026-10-18 — see below**, so P4-2 must keep set-or-delete per field. No fallback needed (report §4 INV-3). Required by the set-or-delete semantics. **Correction [X] (2026-10-18, P3):** the "delete wins" sub-finding does **not** hold. Measured on a throwaway local v25.3.1 alpha, 300 trials per variant (`ETL-Dgraph-Incremental/scripts/probe_same_predicate_set_delete.py`): existing value, set `NEW` + delete `*` → deleted 281, **set survived 19**; no prior value, same mutation → **set survived 300/300**; set `NEW` + delete of the *specific* old value → deterministic 300/300. The outcome of a same-predicate set+delete in one mutation is therefore **undefined**. The answer to the question asked (both kwargs on one mutation) stands, and the conclusion it fed (P4-2 keeps set-or-delete per field) is now a **hard requirement**, not a preference — see P4-2 | V3 |
| ☑ P0-5 | Python | **INV-4:** Spike — can gRPC gzip be enabled via raw channel options under pydgraph? **[X]** Absent from its API; **[V]** always on in Go. **Answered YES (2026-10-16):** `DgraphClientStub(addr, options=[("grpc.default_compression_algorithm", grpc.Compression.Gzip.value)])` works; wire bytes ×0.039 on a synthetic payload; the alpha accepts it (report §4 INV-4) | V4 |
| ☑ P0-6 | Ops | **INV-5:** Document the schema-application procedure (manual today) and verify whether the deployed schema matches `schema.dql`. **[X] Done 2026-10-16:** procedure documented (report §3 INV-5); the deployed schema **matches** `schema.dql` (44 predicates, type `Event`). **[A]** Not compared with the schema inside image build `165463`; re-applying onto a populated alpha is untested. Exercised locally at P4-11. **[X] 2026-10-19 (P4-11):**<br>• **Locally:** works as documented on a fresh and on a populated alpha; the populated case is no longer untested.<br>• **The cited "report §3" / "rev 1 §3" does not exist in the P0 report as it stands (rev 5).** Only the one-line chain in §4 survives.<br>• **Now captured as code:** `ETL-Dgraph-Incremental/scripts/schema_apply.py`, with `verify_schema.py` copied from P0's scratch directory byte-identical.<br>• **The owner should confirm** that step 1 ("snapshot") means a schema snapshot, not a data backup, before P6-0b relies on it | V5 |
| ☑ P0-7 | AWS | **INV-6:** Confirm Fargate networking — is an interface VPC endpoint for `com.amazonaws.ap-southeast-3.secretsmanager` required (private subnets without NAT)? Extend to ECR, ECR-DKR, S3 (layer pulls) and CloudWatch Logs endpoints. **[X] Answered 2026-10-16:** **no new endpoint is required** — the VPC already has Interface endpoints for `secretsmanager`, `ecr.api`, `ecr.dkr` and `logs` (private DNS on), an S3 gateway endpoint and a NAT; the task subnets route via IGW or NAT (report §4 INV-6). Production networking not assessed | V6 |
| ◐ P0-8 | Ops | **[X] 2026-10-20: partly resolved — see §9.0.4 and `INV7_BULK_LOAD.md` in `deploy/staging/`; H1–H6 open before P6-0b.** **INV-7 — DEFERRED [C].** Not investigated or executed now. **Scheduled for the point where Local validation is complete and Staging implementation is about to begin** — due before **P6-0b**, the Staging bootstrap, and **not before**. It **does not block any Local task** (P1, P3, P4, P5.1–P5.3) and is **not** part of P0's acceptance. Unresolved items preserved verbatim in §9.0.4 | V7 |
| ☑ P0-9 | Data | **INV-8:** Run `SELECT COUNT(*)` over `eventIDsQuery` and `linkEvent1IDsQuery` against Production. Converts the **[A]** 2–3× amplification into a verified figure. **[X] Done 2026-10-16 (run 3, exit 0):** Production `eventIDsQuery` **10,093**, `linkEvent1IDsQuery` **219,368**, sum **229,461**; pending 192,016 events / 12,455 links. The 2–3× assumption is **not confirmed as stated** (0.053× / 17.6× / 1.12× combined). **[A]** Environment identity corroborated, not proven by the database; one day only; timing relative to the daily ETL unknown (report §4 "INV-8 — Production") | §6.1 |
| ☑ P0-10 | Ops | **INV-9:** Record the **deployed Dgraph alpha version** so the local container (§7.2) and the pydgraph client version are pinned to match it. **[X] Answered 2026-10-16:** **v25.3.1** (`check_version` and `/health`); pin the local stack to `dgraph/standalone:v25.3.1` (tag exists); pydgraph 25.2.0 verified against it (report §4 INV-9) | new |
| ☑ P0-11 | AWS | **INV-10:** Dump the full JSON of the **latest ACTIVE** `GAR-Stg-Trace-Task-ETL-Dgraph` revision (§8.4 V-a, via §8.6 step 1). **[X] Done 2026-10-16:** only ACTIVE revision is `:1` — `X86_64`, 8 vCPU / 16 GB, Go `:latest`, empty `environment` and `secrets` (report §4 INV-10) | new |
| ☑ P0-12 | AWS | **INV-11 — closed.** `stg/etl-dgraph/creds` is populated (seven `db_*` keys), ARN provided, encryption **[C]** D1 AWS-managed, contract recorded in §8.5. **[X]** All seven key names present and a working Postgres connection verified 2026-10-16 (report §5 V-c); the end-to-end task path is first exercised at P5-21 | new |
| ⏸ P0-13 | AWS | **INV-12:** **[C]** D3 confirms the `dgraph-incremental` tag is **unused and available for the first push**. Still to verify, and only before the **second** push: whether repository **tag immutability** is enabled (V-d), and whether the **lifecycle policy** can expire the Go rollback image (V-i). **[X]** `dgraph-incremental` confirmed absent in ECR (2026-10-16). **⏸ Deferred [C]** (owner-confirmed 2026-10-16: the current INV-12 condition is safe). **V-d** (tag immutability) is due **before the second push** to `dgraph-incremental`; **V-i** (lifecycle policy cannot expire the Go rollback image) is due **before P7-3**. Neither gates the first deployment and neither is part of P0 acceptance | new |
| ☑ P0-15 | Ops | **INV-14 — closed.** Staging `DGRAPH_ALPHAS` = `172.30.0.117:9080` **[C]** (§8.4.1). **Corrected 2026-10-16 (owner-confirmed):** the earlier value `172.30.4.114` is the EC2 instance `gar-hara-redis-dev`, not Dgraph; the alpha is `alpha-2026`, whose own `/health` reports `172.30.0.117:7080` (report §4 INV-14) | new |
| ~~P0-17~~ | — | ~~INV-16 — how `sync_time` returns to `NULL`~~ — **[C] withdrawn (PD-20):** external to the Dgraph ETL; not investigated or required here | — |
| ☑ P0-14 | Ops | **INV-13 [R] opportunistic:** Is **Dgraph ACL enabled** on the Staging cluster? **[C]** C2 — proceed assuming not; this is an **unverified assumption** (§8.5 A4), not a gate. Cheapest moment to disprove it is during the P0-4 pydgraph spike, against a non-production alpha. **[X] Answered 2026-10-16:** an unauthenticated read was **accepted** on the Staging alpha — ACL is **not enforced**; A4 is now an observation, not an inference (report §4 INV-13) | new |

### Deliverables
- `ETL-Dgraph-Initial` created at the PD-21 path with intact history (**[C]** cloned, not renamed)
- Investigation report answering INV-3 … INV-14, **excluding INV-7** — **[C] deferred to the Local→Staging boundary** (P0-8). INV-1/INV-2/INV-16 withdrawn; INV-11 and INV-14 closed; INV-13 opportunistic
- Verified ID cardinality figures
- Verified Staging deployment inventory (§8.4)

### Acceptance criteria
- ☑ `git log` in `ETL-Dgraph-Initial` resolves **`b487294`** and its ancestors — proving the clone preserved history rather than re-initialising it **[X]**
- ☑ INV-3 answered yes, or a documented fallback (two sequential mutations) is agreed **[X]** YES — no fallback needed. *(The "delete wins" detail recorded with it was later disproved — see the P0-4 correction; the criterion's own question is unaffected.)*
- ☑ Measured ID cardinality recorded and compared against the §6 projection of ~200–300k **[X]** Production: 10,093 / 219,368 / sum 229,461 — the sum and the link set fall inside the projection, the event set is far below it. Staging figures (666,214 / 610,268 / 1,276,482) retained as supplementary
- ☑ INV-9 … INV-11 and INV-13 answered (INV-13 **[R]** opportunistic, not a gate; **INV-12 deferred under P0-13**; **INV-7 deferred — explicitly not required for P0 acceptance**), and §8.4's **V-a, V-b, V-c, V-e, V-g** resolved before P5.4 begins. **V-d** is due before the *second* image push and **V-i** before P7-3 — neither gates the first deployment. **[X]** INV-9, INV-10, INV-11, INV-13 answered and V-a, V-b, V-c, V-e, V-g resolved (2026-10-16). **[C]** INV-12's deferral confirmed by the owner (2026-10-16)
- ☑ `stg/etl-dgraph/creds` populated by the owner per the §8.5 specification, with its full ARN recorded **[X]** seven keys present, ARN matches (report §5 V-b, V-c)

> **[R] Stop-and-reassess trigger:** if INV-8 returns **>1M IDs**, the in-memory design (assessment decision 6) must be revisited in favour of keyset pagination per §6.1 before P4 begins. Data-driven, not a reopened decision. **Outcome (2026-10-16): not triggered on Production** — sum 229,461, largest single set 219,368, both <1M. A Staging snapshot read 1,276,482 (sum) but Staging's data lifecycle differs and it is not a Production daily workload. **[C] Resolved 2026-10-18 (PD-22):** the owner's decision is that the ID backlog is **never held in memory**, so the worst-case / recoverability reading *is* a requirement. The in-memory design is replaced by a streamed, disk-backed one (§7.0); keyset pagination is **not** adopted. The trigger is closed, and P4 is no longer gated on it

---

## 4. P1 — Parity Harness (Go) · *Local*

**Objective.** Build the test oracle that does not exist today, **in Go, against the current implementation**. Per assessment §3, tests written after the port would certify the port against itself.

**Dependencies.** P0-1.

**Repo.** `ETL-Dgraph-Initial`.

### Tasks

| ID | Layer | Task |
|---|---|---|
| ☑ P1-1 | L1 | Unit-test pure functions: RDF literals, `buildLinkFacets`, `chunkIndices`, normalizers. **[V]** No infrastructure required. **[X] Done 2026-10-18:** `parity_l1_test.go`, 8 case-table goldens in `parity/golden/l1/` (string/int/float literal, normalizer, facets, chunkIndices, cleanup RDF, lookup JSON) |
| ☑ P1-2 | L2 | Implement a fake `dgraph.Adapter` capturing RDF payloads. **[V]** Returns plain types (`*DQLResponse`, `map[string]string`, `error`). **[X] Done 2026-10-18:** `parity_fakegraph_test.go` — captures raw payloads and also applies them to an in-memory graph (delete-wins per INV-3) for lookups and end-state assertions |
| ☑ P1-3 | L2 | Golden-file tests over `upsertEvents`, `upsertLinks`, `deleteExistingLinks`, `cleanupMissingLinks`. **[V]** All take plain structs — no DB needed. **[X] Done 2026-10-18:** `parity/golden/l2/` (7 files, incl. batched upsert). **Note:** invariant (b) lives in `syncLinks`, which *does* need a DB (`*sql.Rows`); covered at L2 by a stub `database/sql` driver (`parity_stubdb_test.go`) |
| ☑ P1-4 | L2 | Explicitly cover the four silent-corruption invariants: set-or-delete per field; chunk-by-`event1_id` link replacement; facets only for non-empty values; existing-uid vs new-blank-node subject selection. **[X] Done 2026-10-18:** `parity_invariants_test.go` (`TestInvariantA…D_*`, file:line cited). Each shown to **fail under a deliberate mutation** — 6 mutants, re-run independently on the committed code 2026-10-18, control passes (`parity/README.md` §Mutation evidence) |
| ☑ P1-5 | L2 | **Delete-payload coverage for the removal path.** Assessment §4.2: this is what Staging cannot validate without a restore. **[X] Done 2026-10-18:** `l2/delete_nodes.json` (raw + canonical `<uid> * * .` payloads), `l2/remove_invalid_events.json` (full `removeInvalidEvents` via stub DB), plus scenario S3 at L3 |
| ☑ P1-6 | L3 | Stand up a reproducible Postgres fixture, seeded with **synthetic data only** **[C]**. **This fixture is also the local stack's database (§8.2)** — build it once, use it in both languages and both layers. **[X] Done 2026-10-18 (Go side):** `parity/fixture/` — generator `gen/main.go`, PostgreSQL 15.17, `SYN-` marker enforced by `CHECK` constraints, drift test. **Not yet verifiable:** reuse by the Python side and the local stack happens at P4-9/P4-11. **[A]** `sync_time` type and `mv_subject_hierarchy` shape assumed (not in the source DDL). *Cross-reference note: the local stack is §7.2 (P4-10/P4-11), not §8.2* |
| ☑ P1-7 | L3 | Seed all **seven scenarios** **[C]**: new event · updated event · **invalidated event** · link added · link invalidated · event2-only change · event with no links. **[X] Done 2026-10-18:** S1–S7 in `parity/fixture/scenarios.json` with hand-derived expectations that the L3 tests assert independently of the goldens; all matched Go on first run |
| ☑ P1-8 | L3 | Expected-rowset tests for `eventIDsQuery`, `linkEvent1IDsQuery`, `removalQuery`, `eventDetailsQuery`, `linkDetailsQuery`. **[R]** These port to Python unchanged — the SQL strings are identical. **[X] Done 2026-10-18:** `parity/golden/l3/sql/` (5 files, SQL text + `sql_sha256`). Values are recorded *as the Go ETL scans them* (`value_encoding`), so the Python side must map its driver types to that encoding |
| ☑ P1-9 | — | Export golden files in a language-neutral format; stamp each with the generating commit SHA. **[X] Done 2026-10-18:** 28 JSON goldens, format `dgraph-etl-parity/golden/v1` (spec: `parity/README.md`). **Interpretation recorded:** `generator_commit` = `b487294` — `HEAD` at generation, i.e. the commit of the ETL code that produced the output (a test proves ETL sources are unchanged since it). The generating *test code* itself was first committed in `79a38f0`. Export into `ETL-Dgraph-Incremental` is P3-1, not done here |

### Deliverables
- `go test ./...` suite covering L1–L3
- Committed golden-file corpus with SHA provenance
- Fixture database reproducible from a script, from synthetic generators only

### Acceptance criteria
- ☑ `go test ./...` green on `ETL-Dgraph-Initial` **[X]** 2026-10-18, from a fresh clone of `79a38f0` with L3 required (`PARITY_REQUIRE_L3=1`): 127 passes, 0 skips, 0 failures; `go vet` clean. Without a database, L3 tests skip by design
- ☑ All 7 scenarios represented in the fixture, each with at least one golden file **[X]** `parity/golden/l3/pipeline/S1…S7_*.json`, plus `ALL_all_scenarios.json`
- ☑ Scenario 3 (invalidated event) produces captured **delete** RDF, not only set/del triples **[X]** removal-step `DeleteRDF <uid:SYN-S3-E01> * * .` in `S3_invalidated_event.json`, asserted by a test
- ☑ Every golden file cites a resolvable commit SHA **[C]** — history preserved by the seeding push (H1), with `b487294` as the baseline **[X]** all 28 cite `b487294e04cd…`; `git cat-file -t` → `commit`; ETL source diff `b487294..79a38f0` is empty. **Limit:** resolvable **locally only** — no remote exists, so no one else can resolve it yet (§3.0 trigger table)
- ☑ Fixture DB rebuilds from scratch via a single documented command **[X]** `parity/fixture/rebuild.sh` — container destroyed and rebuilt from the clone's own script, suite then green
- ☑ The fixture is generated, not extracted: **no row traceable to Staging or Production** **[C] (PD-3)** **[X]** all rows authored in `parity/fixture/gen/main.go`; every identifier `SYN-`-prefixed and `CHECK`-enforced (a non-marked insert is rejected — tested); no unmarked identifier in the seed or the goldens

**P1 verification record (2026-10-18).** Verified independently of the completion report, against a fresh `git clone` of branch `p1-parity-harness` at `79a38f0` (Windows checkout, `core.autocrlf=true`; `parity/` checks out LF via `parity/.gitattributes`). The commit touches only `parity/**` and 8 new `internal/etl/incremental/parity_*_test.go` files; no ETL source changed. Formal phase exit remains the owner's recorded act.

**Discrepancies and open items carried forward from P1** (none reopens a P1 criterion):
- **Commit location:** `79a38f0` was fast-forwarded onto **`main`** on 2026-10-18 (branch `p1-parity-harness` retained). It is still **unpushed** (no remote): until a remote exists, the corpus lives on one machine only (§3.0 [R]).
- **"Byte-for-byte" (P4 acceptance, §2.1 exit) is defined over the canonical form** in `parity/README.md`: lines sorted, uids → `<uid:UNIQUE_ID>`, blank labels → `_:new:UNIQUE_ID`. Raw byte order is not reproducible even Go-vs-Go (map iteration, SQL row order). The P4 criterion should be read with that definition.
- **Two plan copies:** `ETL-Dgraph-Initial/REFACTOR_PLAN.md` is a stale pre-P0 snapshot; this file is the live one. The §3.0 [?] "which copy is canonical" is still undecided.
- **Behaviour encoded as specification, pending owner confirmation that the port must preserve it:** events with no valid link are never synced or stamped (S7); links with an unresolved endpoint are dropped but stamped; a malformed lookup response reads as "not found". Details: `parity/README.md` §Observed Go behaviour.
- **Not encoded — open data question:** whether `mv_subject_hierarchy` (and `master_mill` beyond its `GROUP BY`) is unique per key in production. If not, `eventDetailsQuery` emits duplicate rows whose outcome in Go depends on batching.

> **This phase gates all Python work.** No P3 task starts until acceptance is met.

---

## 5. Orchestration — out of scope **[C] (PD-14)**

**[C] Step Functions orchestration is owned entirely by the owner.** This plan does not investigate, request, or modify the state machine, and **no orchestration behaviour is a prerequisite, dependency, or blocker** for any task, gate, or acceptance criterion here — including **preventing the Incremental ETL from running when Traceability fails**, which the owner handles.

**The former P2 is withdrawn in full.** P2-1 … P2-8 (insert the Go task, verify `.sync`, verify halt-on-failure, retry, timeout, concurrency guard, alarms, out-of-band rule) and P2-9 … P2-11 (matview refresh, withdrawn earlier by D2) no longer appear as tasks. **P6 no longer depends on them.** INV-1 and INV-2 are withdrawn with them.

### 5.1 What this plan still owes the owner

The ETL-side contract, so the state machine can be configured without reading the ETL source. This is a **handoff artefact, not a dependency** — nothing here waits on it being consumed.

| Property | Value |
|---|---|
| Invocation shape | One-shot batch container; no ports, no server, no readiness probe |
| Success signal | Process **exit code 0** |
| Failure signal | Any **non-zero** exit code; the task reports it as a stopped container with a non-zero exit |
| Retry safety | **Safe at any point.** **[V]** `sync_time` is stamped only in the final step, so an interrupted run reprocesses its whole changeset and a re-run is never destructive |
| Distinguishable failures | **[R]** P5-12 emits distinct exit codes for *config/secret* vs *data/graph* failures |
| Observed duration | Recorded per phase during P6 (P6-4) and handed over for whatever timeout the owner sets |
| **Concurrency — the one hard constraint** | **Must not overlap itself.** **[V]** Two concurrent runs race on `sync_time` and on link replacement: `deleteExistingLinks` wipes `<event1_uid> <links> *` before re-adding, so interleaved runs can erase each other's writes |

### 5.2 Consequences this plan records but does not act on

Stated once so they are not rediscovered later. **[C]** None is tracked, mitigated, or gated here.

- **[V]** The Python ETL carries **no self-gate** (assessment decision 9) — `traceability_check.go` is not ported. Ordering is entirely the orchestrator's property.
- **[V]** Missed days are self-healing: the ETL selects on `sync_time IS NULL`, so rows skipped on a halted day are picked up by the next successful run.
- **[V]** The Go binary retains its self-gate, so it behaves differently from the Python one if both are ever run — relevant only to the rollback artefact (§13).

---

## 6. P3 — Python Foundation · *Local*

**Objective.** Stand up `ETL-Dgraph-Incremental` with infrastructure code only — no business logic.

**Dependencies.** P1 (acceptance), P0-4, P0-5, P0-7. **[X] Status 2026-10-18: all tasks and acceptance criteria met, except live Secrets Manager resolution, which is scheduled for P5-21 (PD-25).**

### Tasks

| ID | Task |
|---|---|
| ☑ P3-1 | Create the repository at `D:\Work\Dattabot X HARA\Repository-GitHub\ETL-Dgraph-Incremental` (**[C]** PD-21) — a **new** repository, **[C]** no history to preserve. Establish layout consistent with sibling Python ETLs **[X]**. §3.0 covers the remote, which is not required to start **[X] Done 2026-10-18** (`9c8603b` … `c0571e0`): new repository at the PD-21 path, `git init`, with `.gitignore` / `.dockerignore` committed **first**. Layout taken from `Refresh-Materialized-Views` and `Sustainability-Assessment-Cycle` (`main.py`, `src/bootstrap.py`, `src/config.py`, exact-pinned `requirements.txt`, `python:3.12.3-slim`, `.env.example`, `tests/`). **Deviations:** a committed `config/app.yaml` of non-secret defaults instead of a gitignored `config.yaml` + `CONFIG_ENV` block; and `.dockerignore` excludes `.env` — **[V]** the siblings' does not, and their `COPY . /app` can bake a local `.env` into an image |
| ☑ P3-2 | **Move canonical `schema.dql` here** **[C]** (assessment §2.3) **[X] Done:** `schema.dql` taken from `ETL-Dgraph-Initial@79a38f0` (blob `c5a038b`) and byte-identical to it (LF). The Go copy stays until P7-9 |
| ☑ P3-3 | Config loader: YAML + env for non-secret tunables (batch sizes, concurrency, chunk size) **[X] Done:** `src/config.py` + `config/app.yaml`. Same variable names as Go, Go's `ApplyDefaults` fallbacks, `DGRAPH_ALPHAS` required (comma-separated). Unknown YAML keys are rejected, so a secret cannot ride along. A set-but-non-integer variable is a startup error (Go ignores it) |
| ☑ P3-4 | Secrets Manager integration, region `ap-southeast-3` **[C]**; fetch **once at startup**, not per connection. **Never** bundle `.env` **[X] Done; verified against a stub, not live (PD-25):** the seven `db_*` keys, all required; fatal, names-only errors; `db_port` from `"5432"` or `5432`; `username`/`password` fallbacks; one `GetSecretValue` with the region pinned (`client.meta.endpoint_url` = `secretsmanager.ap-southeast-3.amazonaws.com`); fetched once, in `bootstrap`, never per connection (a structural test asserts `db.py` cannot reach it). **Live resolution against the real secret is scheduled for P5-21** |
| ☑ P3-5 | **Credential-source switch** for local development: `SECRETS_SOURCE=aws` (default, used in every deployed environment) or `secrets_source=env` (local only, reading a **gitignored, never-COPY'd** `.env`). **[R]** This preserves assessment decision 10 — the image still contains no credentials and the deployed default is unconditionally Secrets Manager **[X] Done:** the variable is the upper-case `SECRETS_SOURCE` (the lower-case form in this row was a typo). Read from the process environment only — never from YAML or the `.env` file — and never falls back between sources. In an image built by the Dockerfile `build_info.DEPLOYED` is `True` and `env` is refused. Demonstrated by `scripts/demo_deployment_image.sh` (12 checks) |
| ☑ P3-6 | `psycopg2` `ThreadedConnectionPool` **[C]**. **[R]** Connections are not safe for concurrent use across threads **[X] Done:** `src/db.py` `ConnectionPool` over `ThreadedConnectionPool`. **[V]** psycopg2's pool *raises* when exhausted, so checkout blocks on a semaphore; one connection per thread is enforced (`OwnedConnection`); connections are recycled on checkout past `max_life_time_ms` |
| ☑ P3-7 | Server-side (named) cursors for the large ID queries **[X] Done:** `stream_rows` / `stream_column` — a named cursor fetched `itersize` rows at a time; a test proves the result set lives server-side (`pg_cursors`) and is closed on exhaustion or early exit. This is the primitive PD-22 builds on |
| ☑ P3-8 | pydgraph **synchronous** client wrapper **[C]**, version pinned against INV-9; apply INV-3/INV-4 findings **[X] Done:** `src/dgraph_client.py` on pydgraph **25.2.0** (the newest release; verified against the deployed v25.3.1) with `grpcio` / `protobuf` pinned. INV-4 gzip applied and accepted by a real alpha. INV-3: set and delete travel in **one** mutation. **INV-3 correction applied:** same-predicate set+delete is *undefined* (see P0-4), so the wrapper's documented contract is *never send both for one field*. 15 live tests against v25.3.1 |
| ☑ P3-9 | Re-implement the retry classifier — **[V]** `isRetryableError`, gRPC codes `Aborted`/`Unavailable`/`DeadlineExceeded`/`ResourceExhausted` plus ~18 string patterns **[X] Done:** `src/retry.py` — 4 gRPC codes + 18 patterns, table-tested with one case per code (all 17) and per pattern, and cross-checked against the Go source text. pydgraph's `RetriableError` / `ConnectionError` lose `.code()` and are unwrapped. Real `grpc` errors (closed port → `UNAVAILABLE`; a genuine aborted transaction) are classified in tests |
| ☑ P3-10 | Thread fan-out primitive replacing `fanOutBatches`. **[V]** Config defaults are `concurrency: 4`, batches 50–100 — a small `ThreadPoolExecutor` suffices. **[R]** One pydgraph `Txn` per thread **[X] Done:** `src/fanout.py`. **Correction to this row:** **[V]** Go's config defaults are upsert batches **10** and other batches **100**; **50 is only the zero-value fallback** (`ApplyDefaults`). Both are reproduced. A cancelled fan-out never reports success |
| ☑ P3-11 | Structured JSON logging to stdout (CloudWatch-compatible); `PYTHONUNBUFFERED=1` so no line is lost on exit **[X] Done:** one JSON object per line on stdout with Go's key names. **[V]** `LOGGER_LEVEL` defaults to `debug` and botocore at DEBUG logs HTTP response bodies, i.e. the secret — so botocore / boto3 / urllib3 / grpc loggers are clamped to WARNING and registered secret values are redacted. `PYTHONUNBUFFERED=1` is set in the Dockerfile |
| ☑ P3-12 | Port L1 tests from P1-1 **[X] Done:** `tests/test_l1_golden.py` — all 8 L1 goldens; 101 cases reproduce Go exactly, and 1 is marked not-required-of-the-port by P1. This needed the pure helpers in `src/rdf.py` / `src/helpers.py` (no pipeline logic) |
| ☑ P3-13 | **Do not port** `StringInterner` **[C]** (§6.1) or `traceability_check.go` **[C]** (assessment decision 9) **[X] Verified:** guard tests inspect the code with `ast` (so explanatory docstrings do not trip them) and assert there is no interner, traceability gate, `APP_MODE`, replica variable, S3 client, Dgraph credential handling, materialized-view code or tz handling |

### Deliverables
- Repository scaffold with config, secrets, DB pool, Dgraph client, concurrency primitive
- L1 test suite passing in Python

### Acceptance criteria
- ☑ L1 parity: Python reproduces every P1-1 expected value **[X]** 101 of 102 recorded cases (the 102nd is P1's own "not required" case); the goldens are checksummed in `tests/golden/MANIFEST.sha256`
- ☑ The loader reads the seven `db_*` keys from `ap-southeast-3`, fetched once at startup; **no `.env` in the image or repository** **[X]** verified against a **stubbed** boto3 client in the pinned region, with no network call. *(Re-scoped 2026-10-18 by **[C]** PD-25: the criterion originally read "Secrets resolve from `ap-southeast-3` at runtime", which can only be proved against the real secret. Per the owner that is **P5-21**; nothing before it reads a real secret, and §2.1 keeps Local free of AWS.)*
- ⏸ **Live resolution of `stg/etl-dgraph/creds`** — **[C] deferred to P5-21** (PD-25), the same pattern as P0-8 / P0-13: scheduled, not dropped, and **not** part of P3 acceptance
- ☑ `SECRETS_SOURCE` defaults to `aws`; an image built for deployment cannot be made to read a bundled file **[X]** `scripts/demo_deployment_image.sh`, 12 checks: five decoy credential files planted in the build context never reach the image; `env` is refused with a valid `.env` mounted, case-insensitively, and when written *inside* the mounted file; AWS mode never falls back to the file; and a build whose `.dockerignore` fails to exclude them **fails** with "credential file(s) in the image"
- ☑ Connection pool survives a concurrency soak at 4× configured workers **[X]** against the synthetic fixture: 16 threads (4× the 4 configured workers) and 40 threads (4× the pool of 10), 30 checkouts each, zero errors, never more than 10 checked out
- ☑ Retry classifier reproduces Go's verdict across the full gRPC-code and string-pattern matrix **[X]** all 17 codes and all 18 patterns, plus a cross-check of the lists against the Go source
- ☑ No tzdata dependency **[V]** — removed with the gate **[X]** asserted over code, requirements, Dockerfile and the installed distributions

**P3 verification record (2026-10-18).** Re-verified independently of the completion report: `ETL-Dgraph-Incremental` at `201c0c9` (`c0571e0` is the P3 code; `201c0c9` adds the shared-fixture wrapper); **527 tests pass, 1 skipped** with the synthetic Postgres fixture, a throwaway local Dgraph v25.3.1 alpha and the sibling repositories all live (`REQUIRE_LIVE=1`); a fresh clone of `c0571e0` gives 518 passed / 3 skipped (the two cross-repo checks need the siblings mounted). **22 deliberate mutants** across retry, credentials, pool, Dgraph client, fan-out, helpers, logging and config were **all caught**, after one initial survivor was fixed. The Go P1 suite is still green (`go test ./...`, L3 live) and its ETL sources are unchanged since `b487294`. **Not covered by P3:** a real Secrets Manager call (P5-21), a real Staging alpha, and any pipeline logic (P4).

**Findings from P3 that change later phases** (both measured on a local v25.3.1 alpha; probes in `ETL-Dgraph-Incremental/scripts/`):
- **INV-3 correction** — see P0-4 and P4-2.
- **Best-effort lookups are stale for up to ~1 s.** **[V]** Go's uid lookups use `WithBestEffort()`. Read back-to-back, 46–69 % missed a node committed just before (time-to-visible p50 145 ms, p90 930 ms, max ~1.0 s, never permanent); a normal read-only query missed 0/300. In a daily run this is harmless — seconds separate the steps and the uid map covers nodes the run created. It **does** bite a local end-to-end harness that seeds the graph and runs the ETL immediately — see P4-12.

---

## 7. P4 — Port Incremental Logic + Local End-to-End · *Local*

**Objective.** Port the business logic, validating each step against P1 golden files, then prove the whole pipeline end-to-end against a local Postgres **and** a local Dgraph.

**Dependencies.** P3 (acceptance) — **met 2026-10-18** (live Secrets Manager resolution is scheduled for P5-21, PD-25). The gate check is recorded at the end of this section.

**[X] Status 2026-10-19: §7.1 (the port, P4-1 … P4-9, P4-15 … P4-19) is complete and verified** — see the P4 §7.1 verification record below. **[X] §7.2 (P4-10 … P4-14, the local stack and end-to-end against a real Dgraph) is complete as of 2026-10-19 (Stage 4)** — see the §7.2 verification record below. All P4 acceptance criteria are met. **[C] P4 closed by the owner (2026-10-19)**, accepting both P4-13 readings: for SIGKILL, the last `phase started: events` line is sufficient; for an unreachable Postgres, the `startup failed` line is sufficient, with no change.

**[R] Order is prescribed:** **`links` is last** — it carries the chunk-by-`event1_id` rule, the subtlest invariant in the codebase.

### 7.0 Memory design — streamed, disk-backed **[C] (PD-22)**

**Requirement (owner, 2026-10-18).** The ID backlog is never held in memory. This supersedes assessment decision 6 and §6's verdict "in-memory ID sets are safe", and closes the §3 stop-and-reassess trigger. Today's volumes would have fitted in memory — the requirement is that **resident memory must not scale with the backlog**.

**What Go keeps for the whole run [V]:** the event ID set (`events.go:177`); the event1 ID set (`links.go:113`); `uidMap`, which only grows and is passed from the events step to the links step; the link-ID slice and the event/removal ID sets that the single final `sync_time` stamp names; and the ID list `cleanupMissingLinks` subtracts from. All of them grow with the backlog.

**Options evaluated against assessment §6.1** (measured 2026-10-18, Python 3.12, synthetic 36-character ids, at the Staging snapshot: 666,214 events + 610,268 event1 ids + 805,638 link ids):

| Design | Resident memory | Disk | Verdict |
|---|---|---|---|
| In-memory (Go ported 1:1) | **+240 MB** over a 16 MB baseline, linear — about 1.9 GB at 10 M ids | none | Meets nothing the owner asked for; kept only as the **reference implementation** the tests compare against |
| Keyset pagination in SQL (§6.1's named fix) | O(page) | none | **Rejected as the default.** It re-executes the whole multi-CTE query for every page — **[X]** P0 measured the ID queries on **Production** (run 3): `eventIDsQuery` 22.5 s and `linkEvent1IDsQuery` **316.6 s** as bare `COUNT`s, so paging the link query in 10,000-id pages would re-run a ~5-minute statement about 22 times (~2 h); an earlier simpler count varied 43–947 s for an unverified cause — and it changes the SQL that P1-8 / P4-9 pin byte-for-byte. Remains the documented fallback if the ledger ever proves unworkable |
| Streaming only, no ledger | O(chunk) | none | **Not sufficient.** The single final stamp (P4-6) must name the exact rows processed; "stamp everything still NULL" would also stamp rows re-nulled mid-run that were never processed |
| **Streamed ingest + disk-backed run ledger** (SQLite, standard library) | **+57 MB, constant** | **103 MB** at Staging scale | **Chosen.** Load 11 s for 1.28 M ids plus a uid cache; a full scan plus anti-join 0.3 s; 5,000 point lookups 23 ms |

**Chosen design — spill, then process.**
1. **Ingest.** Run each ID query **unchanged** (`eventIDsQuery`, `linkEvent1IDsQuery`, `removalQuery`) through the P3-7 server-side cursor, writing batches into ledger tables. The source transaction is read-only and ends when ingest ends, so **no cursor stays open during the Dgraph work**.
2. **Process.** Each phase reads chunks of `fetch_chunk_size` from the ledger in key order. Memory is O(chunk). The chunking invariants are untouched: a link chunk is a chunk of **distinct `event1_id`s**, and `linkDetailsQuery` fetches by `= ANY(chunk)`, so one event1's links are never split.
3. **Run state also lives in the ledger:** `uids` (`unique_id → uid` for every uid learned this run — lookup results and blank-node results — behind a small in-memory LRU), the link IDs seen, and the event1 IDs handled. `cleanupMissingLinks` becomes a SQL anti-join: events processed **minus** event1s handled, with a uid known.
4. **Stamp.** After every phase has succeeded, read the processed IDs back in batches of 200 — **unchanged** guarantee (P4-6). The ledger is scratch: lost on a crash by design, because `sync_time` is untouched until the end and a re-run re-derives it. It is deleted on normal exit.

**Parity consequences.** Chunk order differs from Go (a Go map iterates in random order; the ledger is key-ordered) — **[X]** the end state is independent of chunking (P1's L3 pipeline test reaches the identical graph with every size at 1 and four workers), and the goldens use one batch. New-node uids are kept for the whole run, exactly as Go's `uidMap` does — this matters because of the ~1 s best-effort staleness (P3 findings): a node created this run can be invisible to a best-effort lookup.

**Limitations — recorded, not hidden.**
- **Ephemeral disk.** About 100 MB per 1.3 M ids. **[V]** P5-19 said the Python ETL "writes no files"; that is no longer true and is corrected there. Default Fargate ephemeral storage is 20 GiB; the registered revision's value must be confirmed (P5-19).
- **Complexity and test surface:** a new module plus P4-15 … P4-18 (equivalence with the in-memory reference on randomised inputs, a memory-ceiling test, crash safety).
- **Point-in-time IDs:** the ID snapshot is taken at ingest, as in Go (which also reads all IDs before the details).
- **One run at a time (PD-16):** the ledger is private to a run; two concurrent runs were already unsafe.
- **The ID queries stay slow** — **[X]** on Production `linkEvent1IDsQuery` took 316.6 s (P0 run 3), and two earlier runs lost their connection on long statements (cause unverified). Streaming does not speed the queries up; it only avoids holding their results.
- **Not "zero" memory:** a chunk of `fetch_chunk_size` ids with their ~44-column detail rows, the LRU and the interpreter baseline, tens of MB. P5-3 records it.
- **[?] Unverified:** whether the RDS parameter group sets `idle_in_transaction_session_timeout`. The ingest cursor is *active* while rows are written to the ledger, so it should be unaffected; confirm in P6.

### 7.1 Port tasks

| ID | Step | Task | Invariant to preserve |
|---|---|---|---|
| ☑ P4-1 | removal | Port `removalQuery`, uid lookup, node deletion **[X] Done 2026-10-19:** `src/removal.py`, `src/graph_ops.py`; L2 golden `remove_invalid_events` reproduced (writes, raw `<uid> * * .` payloads, graph after) | Delete-by-uid; re-run no-ops **[X]** `test_removal_deletes_by_uid_and_a_rerun_writes_nothing` |
| ☑ P4-2 | events | Port `eventIDsQuery`, chunking (IDs come from the ledger, §7.0), `eventDetailsQuery`, upsert **[X] Done 2026-10-19:** `src/events.py`; L2 goldens `upsert_events` / `upsert_events_batched` and all 8 L3 pipeline goldens reproduced. A cleared field is `<s> <p> *` in the **same** `MutateWithDeletesRDF`, tested for each of the 42 predicates (cross-checked against `schema.dql`); mutants "delete omitted" and "delete in a separate mutation" are caught. **Deviation D-1 — [C] resolved 2026-10-20: Option A, Go parity (M13):** this row says the port *asserts* the no-(subject, predicate)-in-both rule on every mutation. The port **checks** every mutation but, when disagreeing duplicate detail rows trigger it, **logs a warning and sends the mutation as Go does** (PD-23: Go is the specification) rather than failing the run. The **test suite** asserts the rule on every mutation (its fake graph rejects any violation), which satisfies the acceptance criterion below; failing the run in production instead would be a behaviour change from Go | **Set-or-delete per field — a hard requirement, not a preference.** A present value is a set and an absent one is `<s> <p> *` in the same mutation, and **no (subject, predicate) may appear in both the set and the delete of one mutation**: **[X]** Dgraph v25.3.1 resolves that combination **non-deterministically** (P0-4 correction: ~6 % of trials let the set survive a wildcard delete, and the set always survives when no value existed). The port **asserts it on every mutation it emits**; **[X]** none of the P1 goldens' 38 writes (1,369 subject-predicate pairs) violates it. **[?] Open data question:** if `mv_subject_hierarchy` or `master_mill` can yield several rows for one event, `eventDetailsQuery` returns that event several times, and duplicates that disagree (one with a value, one empty) would put a set and a delete for one field into one mutation. The P1 fixture keeps one row per key; production uniqueness is **unverified** — a read-only duplicate count is the cheap check |
| ☑ P4-3 | events | Blank-node labelling for new nodes **[X] Done 2026-10-19:** labels are batch-local per `unique_id` (`_:new0`, `_:new1`, …), not Go's | **[V]** Byte-parity with Go **not** required (assessment §2.2); only batch-local uniqueness and correct `blankToID` round-trip **[X]** asserted by property: one label per `unique_id`, never shared, an existing uid is never a blank node, returned uids round-trip into the uid map and later links use them |
| ☑ P4-4 | links | Port `linkEvent1IDsQuery`, `linkDetailsQuery`, delete-then-write, `cleanupMissingLinks` (the ledger anti-join, §7.0) **[X] Done 2026-10-19:** `src/links.py`; L2 goldens `upsert_links`, `delete_existing_links`, `cleanup_missing_links` reproduced | **Chunk by `event1_id`, never by link row** **[X]** `test_link_replacement_is_grouped_by_event1` fails when chunks are cut by link row (mutant M1) or when only event1s with a valid link are cleared (M8) |
| ☑ P4-5 | links | Facet construction **[X] Done** | Only non-empty values emitted **[X]** each of the six facets omitted when empty; no parentheses when none; whitespace-only values trimmed away then omitted (mutant M4 caught) |
| ☑ P4-6 | sync_time | Port stamping, batched 200 rows per `UPDATE`, reading the processed IDs from the ledger **[X] Done 2026-10-19:** `src/sync_time.py`. Per-batch commits kept as Go (**[C]** PD-26) | **Single final step, after all phases** — the restartability guarantee **[X]** nothing is stamped before `sync_time` begins; failures injected at every graph operation of removal / events / links, a lost Postgres connection and a cancellation all stamp **zero** rows; batches of exactly 200 (mutants M5, M7, M10 caught) |
| ☑ P4-7 | matviews | **Not ported** **[C]** (D2) — MV refresh is removed from the Dgraph ETL and is unrelated to Dgraph output. **[V]** Already removed from Go at `b487294`. What refreshes the views elsewhere is **outside this plan's scope** and is not a dependency of any task here | No `REFRESH MATERIALIZED VIEW` anywhere in the Python repository **[X]** a repository-wide test (every file, code, tests and docs) finds none of the five names |
| ☑ P4-8 | — | **No self-gate** **[C]**; entry point runs removal → events → links → sync_time, then exits **[X] Done:** `main.py` + `src/migrator.py`; the first source call of a run is `removalQuery`; exit 0 / 1, the failing phase named in the log | — |
| ☑ P4-9 | — | Port L3 SQL tests unchanged against the same fixture **[X] Done:** the five rowsets are identical against the Go-owned fixture (PD-24), the SQL text byte-identical (the two `$1` statements run through `PREPARE` / `EXECUTE` unchanged) | — |
| ☑ P4-15 | memory | **Run ledger** (§7.0, PD-22): a private SQLite file in a temp directory — tables for processed event IDs, event1 IDs, removal IDs, link IDs and `uids`; batched inserts; key-ordered chunk reads; the `cleanupMissingLinks` anti-join; an LRU in front of `uids`; deleted on normal exit **[X] Done 2026-10-19:** `src/run_state.py` `LedgerRunState`. **LRU (PD-27):** write-through, positive entries only, table and cache updated under one lock, bounded at `UID_CACHE_SIZE` = 10,000 entries (~2.5 MB); tested for reference-equivalence at capacities 1 / 3 / 50 / 10,000, bounding, LRU order, no stale entry under concurrent re-merges; every L2 / L3 golden and invariant test also runs with a 1-entry cache; 4 cache mutants caught. **Deviation D-2:** the cache size is a constructor default, **not** a configuration knob, so the P3 config contract (§8.4.1) is unchanged. **Fixed during P4:** the stamping union was filtered per chunk outside the `UNION` (quadratic — 64 s at 200 k ids); the bound now sits inside each branch | Resident memory independent of the backlog **[X]** P4-17 |
| ☑ P4-16 | memory | **Streamed ingest:** `eventIDsQuery`, `linkEvent1IDsQuery` and `removalQuery` through the P3-7 cursor into the ledger with the **SQL text unchanged** **[X] Done** | The ledger's contents equal the P1-8 rowsets (asserted in the P4-9 tests); the source transaction is closed before any Dgraph call **[X]** `test_ingest_fills_the_ledger_with_the_rowsets_and_ends_the_source_transaction_first`: ledger tables = the rowsets under Go's ingest rules; Postgres reports no session of the run with an open transaction at any graph call (with a positive control). A leaked stream connection is additionally refused by the pool's one-connection-per-thread guard |
| ☑ P4-17 | memory | **Memory tests:** (a) the ledger pipeline equals an in-memory reference on randomised inputs (same graph writes, same stamped set); (b) a **ceiling test** over a synthetic backlog of ≥ 2 M ids against fakes: **[R]** peak RSS at 2 M ids within +25 MB of the RSS at 200 k **[X] Done:** (a) 40 randomised worlds, identical writes and stamps (default and 1-entry LRU); (b) peak RSS (`VmHWM`) **~45 MB at 200 k → ~46.5 MB at 2 M, +0.8 to +1.6 MB across runs** (bound: +25 MB), 2 M ids in 18–19 s; a control asserts the in-memory reference **does** exceed the bound | No structure proportional to the backlog |
| ☑ P4-18 | memory | **Crash safety:** a run killed mid-ingest, mid-`events` and mid-`links` leaves **zero** rows stamped, and the next run starts from a fresh ledger and reprocesses cleanly **[X] Done:** a real process `SIGKILL`ed at each point; zero rows stamped; the next run, on the graph the dead run left (replayed from its write journal), reaches the golden graph and stamps. The orphaned ledger directory is accepted (**[C]** PD-28). A kill *during* `sync_time` can leave partial stamps (**[C]** PD-26) | Restartability (P4-6) |
| ☑ P4-19 | parity | **Go-parity guard (PD-23).** Tests pin the three behaviours the port must keep: (1) an event with no valid link is **neither synced nor stamped** (scenario S7); (2) a link whose endpoint has no uid is **dropped but still stamped**; (3) a malformed uid-lookup response reads as **"not found"**. Each test cites its follow-up (FU-1 … FU-3, §14.3). **Changing any of them is out of scope for this refactor** and needs an owner decision **[X] Done:** `tests/test_parity_guards.py`; "fixing" FU-2 is caught (mutant M9) | Go's behaviour at `b487294` is the specification |

### 7.2 Local stack & end-to-end (L4)

The assessment's L4 layer, executed locally. This is the first time the Python ETL writes to a real Dgraph.

| ID | Task |
|---|---|
| ☑ P4-10 | **Local stack as code** — a `docker compose` (or equivalent) definition bringing up **Postgres** and a **Dgraph zero + alpha**, alpha version pinned to INV-9. One documented command up, one down, no manual steps **[X] Done 2026-10-19:** `scripts/local_stack.sh up` / `down`.<br>• Postgres: the Go-owned fixture via `scripts/fixture.sh up` (PD-24).<br>• Dgraph: zero + alpha from `local/dgraph-stack.compose.yaml`, `dgraph/dgraph:v25.3.1` pinned by digest; the alpha reports v25.3.1.<br>• `up` then applies the schema (INV-5) and seeds the pre-run graph.<br>• From a machine with none of the stack's images, volumes or containers: `up` exit 0 in 169 s, pulls included; `down` leaves no container, volume, network or generated file |
| ☑ P4-11 | Seed Postgres from the **P1-6/P1-7 synthetic fixture** — **[C]** PD-24: the fixture stays **owned by `ETL-Dgraph-Initial`**, and Python invokes **the Go repository's own script in place** (`scripts/fixture.sh up`) rather than copying it; `tests/fixture.lock` pins its hashes and `up` refuses to run if they differ. **[X]** Done in the P4 preparation (7 tests). Apply `schema.dql` to the local alpha using the **INV-5 procedure**, so the procedure itself is exercised locally before it is relied on in Staging **[X] Done 2026-10-19:** the four documented steps as code (`scripts/schema_apply.py`; `verify_schema.py` copied byte-identical from P0).<br>• **Fresh alpha:** all four steps work as documented (drift, then `/alter` Success, then 44/44 + `Event`).<br>• **Populated alpha (the former [A]):** "already identical", `/alter` Success, data unchanged, index still serves lookups.<br>• **Not as documented:** the P0 report's pointer "rev 1 §3" is dangling, so step 1 is read as a *schema* snapshot, not a data backup — see the §7.2 record |
| ☑ P4-12 | Run the Python ETL from source against the local stack (`SECRETS_SOURCE=env`); assert graph state after each of the four phases. **Harness wait [X]:** the ETL's uid lookups are best-effort and can miss a node committed up to **~1 s** earlier (measured: p50 145 ms, p90 930 ms, max ~1.0 s), so after seeding the graph from `graph_seed.json` **the harness waits about 1.5 s** — or polls one seeded node with a strong read — **before starting the ETL**; otherwise the seeded nodes look "not found" and the ETL creates duplicates. **This is a test-harness wait only: no sleep or delay is added to production code**, and Go's best-effort lookups are kept for parity (FU-4) **[X] Done 2026-10-19:** all 8 pipeline goldens (S1 … S7, ALL) through the real `main.main()`.<br>• After **each** phase, the live graph equals Go's oracle (seed + the golden's captured writes up to that phase), and nothing is stamped before `sync_time`.<br>• The harness wait polls with a best-effort read until every seeded node is visible |
| ☑ P4-13 | Exercise the failure paths locally: kill the process mid-`events`; stop the alpha mid-run; present an unreachable Postgres. Assert **non-zero exit**, a diagnosable log line, and **zero `sync_time` rows stamped** **[X] Done 2026-10-19:** all three, each followed by a retry that reaches Go's end state; details in the §7.2 record |
| ☑ P4-14 | **Synthetic-data and credential guards — in the regular test suite** **[C]** (D4). Ordinary tests in the default suite (`pytest` in the Python repo, `go test` in the Go repo), **not** pipeline steps — so they hold under PD-10 with no CI dependency. Each must run **fully offline**: no AWS call, no network, no real credential, no environment-specific input. Four checks: **(a)** no tracked file matches database-dump or bulk-extract patterns (`*.sql`, `*.dump`, `*.csv`, `*.rdf.gz` outside fixtures); **(b)** no tracked `.env` or credential-bearing file, and no high-entropy literal in tracked source; **(c)** every fixture row carries the synthetic marker, and none matches a real-data heuristic; **(d)** the §8.5 key-contract test — the loader's required-key list equals the seven `db_*` names, asserted against the documented list, **never** against Secrets Manager **[X] Done 2026-10-19:** Python `tests/test_data_guards.py` (a)–(d); Go `parity/guards/guards_test.go` (a)–(c). (d) does not apply to Go: it has no Secrets Manager loader. Both read the tracked list from `.git/index` directly |

### Deliverables
- Complete Python incremental ETL
- L2 + L3 test suites passing in Python
- Reproducible local stack (Postgres + Dgraph) with a one-command bring-up
- L4 end-to-end evidence for all seven scenarios

### Acceptance criteria
- ☑ **Every P1 golden file reproduced byte-for-byte** — the primary equivalence gate. **[R] clarification:** "byte-for-byte" is over each golden's **canonical form** (`parity/README.md` §Canonical form: lines sorted, uids → `<uid:UNIQUE_ID>`, blank labels → `_:new:UNIQUE_ID`), because raw order is not reproducible even Go-against-Go and blank-node labels are not required to match (P4-3) **[X]** all 7 L2 and all 8 L3 pipeline goldens (writes, graph after, stamped rows; lookups informational), on the ledger, the ledger with a 1-entry LRU and the in-memory reference; also identical with every size 1 and four workers. The `delete_nodes` golden's raw payloads match byte-exactly
- ☑ L3 queries return rowsets identical to Go's against the shared fixture **[X]** all five, SQL text byte-identical (sha256 = golden)
- ☑ All 7 scenarios pass **end-to-end against the local Postgres + Dgraph**, not only against golden files **[X]** 2026-10-19: S1 … S7 and ALL, phase by phase, against Go's oracle, through the real entry point (`tests/test_e2e_local.py`)
- ☑ A deliberately interrupted run leaves **zero** rows stamped and reprocesses cleanly on retry **[X]** against fakes and the fixture (P4-6 injection tests, P4-18 `SIGKILL` + recovery) **and against a real Dgraph** (P4-13, 2026-10-19). Each injection stamped zero rows, and a plain re-run on the state it left reached Go's `graph_after` and stamps. That state was a half-written graph after the kill, and a restarted alpha after the stop
- ☑ Every failure injected in P4-13 produces a non-zero exit and a log line naming the failing phase **[X]** 2026-10-19:<br>• **Alpha stopped:** exit 1, `phase failed` + `incremental run failed` with `phase: events`.<br>• **Postgres unreachable:** exit 1, `startup failed … could not open the Postgres pool`. The pool opens before any phase, so the failing phase is startup, named in the message; there is no structured `phase` field.<br>• **SIGKILL:** exit -9; a killed process cannot log, so the diagnosable line is the last marker, `phase started: events`, with no finish.<br>• **SIGTERM** (an ECS stop) exits -15 **without** a line — **P5-13**, pinned by a strict xfail
- ☑ No `traceability_log_daily` reference anywhere in the codebase **[C]** **[X]** repository-wide test over every file
- ☑ No `REFRESH MATERIALIZED VIEW` and no `mv_events` / `monitoring_cache_shipments` / `mv_shipment_list` reference anywhere in the codebase **[C]** **[X]** same test; `mv_subject_hierarchy`, which Go's `eventDetailsQuery` **reads**, is the only `mv_` name present
- ☑ **Guard tests (P4-14) green as part of the default test command** — no separate invocation, no flag to remember **[X]** 2026-10-19: unmarked tests under `pytest` (34) and `go test ./...` (11)
- ☑ Guards pass with **networking disabled and no AWS credentials present**, proving they depend on neither **[X]** `scripts/demo_guards_offline.sh`.<br>• **Environment:** `--network none` (only `lo`; outbound unreachable; DNS fails), no `AWS_*` variable, no `~/.aws`, repositories mounted read-only.<br>• **Results:** Python guards 34/34 and the whole default suite (859 passed); Go guards and the whole `go test ./...` green.<br>• **Enforced inside pytest too:** an autouse fixture makes any socket, DNS or boto3 call raise
- ☑ Each of the four checks **demonstrated to fail** against a planted fixture (negative test per check, not one blanket test) **[X]** both ways:<br>• **In the suites:** per-check negative tests (Python `test_*_negative_*`, Go `TestNegative*`).<br>• **In scratch checkouts with all this work committed:** each of these, planted and `git add`ed, failed exactly the matching guard in both repositories — a `.csv` extract, a `.env.staging`, an AWS-key-shaped literal in source, an unmarked fixture row, and a real-looking company name in a row. A renamed `db_*` key in the documented contract failed (d)
- ☑ The key-contract check (d) fails when the loader's required-key list and the §8.5 table diverge **[X]** already satisfied in P3 (`tests/test_credentials.py`, which also compares against §8.5 when this plan is present); it must stay green **[X]** still green 2026-10-19; P4-14 adds explicit negatives (a dropped, added, renamed or reordered key, and a key marked not-required in the document), all against `docs/SECRET_CONTRACT.md`, never Secrets Manager
- ☑ **Resident memory is independent of the backlog** (PD-22): the P4-17 ceiling test passes, and the ledger pipeline equals the in-memory reference **[X]** +0.8 to +1.6 MB from 200 k to 2 M ids, with the PD-27 LRU in place
- ☑ **No mutation the port emits sets and deletes the same (subject, predicate)** (P4-2), asserted on every mutation in the test suite **[X]** the test fake graph rejects any such mutation, on every mutation of every test; none occurs on the fixture or the goldens. The only way to produce one is disagreeing duplicate detail rows, which the port detects, logs and sends as Go does — **D-1**, open
- ☑ The three Go behaviours are preserved and pinned by tests that cite FU-1 … FU-3 (PD-23, P4-19)
- ☑ A run killed at any phase leaves zero rows stamped (P4-18) **[X]** killed mid-ingest, mid-`events` and mid-`links` (real `SIGKILL`). **Qualified by [C] PD-26:** a kill *inside* `sync_time` can leave partial stamps, which is the accepted Go behaviour and safe to re-run

**P4 §7.1 verification record (2026-10-19).** `ETL-Dgraph-Incremental` at `dd0c20c` (`d2fc26a` the port, `dd0c20c` the PD-27 LRU and the P4-16 test). **898 tests pass, 1 skipped, live** — synthetic Postgres fixture (PD-24), a throwaway local Dgraph v25.3.1 alpha and the sibling repositories mounted, `REQUIRE_LIVE=1`; the one skip is P1's own "not required of the port" L1 case. **14 deliberate mutants** were all caught: 10 P4 invariant mutants (chunking by link row, a missing delete, deletes in a separate mutation, an empty facet, stamping before links, a label per record, batches of 500, clearing only event1s with a valid link, "fixing" FU-2, a swallowed phase failure) and 4 LRU mutants (stale entry, no eviction, FIFO, cached negatives); the P4-16 open-transaction check carries its own positive control. `ETL-Dgraph-Initial` and the golden corpus are unchanged. **Not covered by §7.1:** a real Dgraph end to end, the P4-13 failure paths and the P4-14 guards (§7.2, Stage 4).

**P4 §7.2 verification record (2026-10-19).** Stage 4, against `ETL-Dgraph-Incremental` at `dd0c20c` plus the uncommitted §7.2 work, and `ETL-Dgraph-Initial` at `79a38f0` plus `parity/guards/` and a `parity/README.md` section. Gate re-checked first: Stage 3 complete, P0-6 present, INV-9 recorded (v25.3.1). The INV-5 text is only the one-line chain in the P0 report §4 — see INV-5 below.

*Test results*
- **Python, everything live:** **948 passed, 1 skipped, 1 xfailed**, with `REQUIRE_LIVE=1`.
  - Live services: the local stack, the shared fixture, a throwaway v25.3.1 alpha, and the siblings mounted.
  - The skip is P1's own "not required of the port" case. The xfail is the P5-13 SIGTERM pin.
- **Go:** `go test ./...` green with L3 live, `go vet` clean.
- **Unchanged:** ported logic, goldens, fixture.
- **Changed in existing files:** `scripts/test.sh` passes the e2e variables through, and mounts the Docker socket only when `E2E_DOCKER_SOCKET` is set. Plus additions to `.gitignore`, `.dockerignore` and `pytest.ini`.

*INV-5, exercised.* The P0 report §4 records the procedure as "snapshot → `verify_schema.py` → `POST /alter` → re-verify" and points to "rev 1 §3" for the steps. **That section no longer exists in the report** (overwritten by later revisions).
- **Implementation:** the four steps as written (`scripts/schema_apply.py`). Step 1 is read as a **schema** snapshot (`schema {}` saved to a file), **not a data backup**. The owner should confirm that reading before P6-0b.
- **Fresh alpha:** works as documented.
- **Populated alpha** (the P0-6 [A]): "already identical", `/alter` Success, data unchanged, `unique_id` lookups still served. **Nothing failed, so the ambiguity policy's stop condition was not met.**

*Failure paths (P4-13).* Every injection stamped zero rows, and its retry reached Go's end state.

| Injection | Exit | Log | Time to fail |
|---|---|---|---|
| (a) SIGKILL after the first event mutation | -9 | last marker `phase started: events` | — |
| (b) alpha `docker stop`ped mid-`events` | 1 | `phase: events`, `UNAVAILABLE` | 1.6–1.7 s with `DGRAPH_MAX_RETRIES=3` |
| (c) Postgres refused | 1 | `startup failed … could not open the Postgres pool` | 0.8–1.1 s |
| (c) Postgres black-holed | 1 | the same | 3.8–4.0 s |

With the **default** `DGRAPH_MAX_RETRIES=100`, a lost alpha retries for about 8 minutes before a mutation fails (computed from `retry.py`, not measured). That figure matters for the owner's timeout (§5.1).

*Peak RSS — the first comparison point for §6 and P6-3.* Measured as the ETL process's own VmHWM, as in P4-17.
- **The fixture (~30 rows):** **50.2–50.6 MB**.
- **`scripts/local_stack.sh measure`:** **64.9 MB** on a synthetic scale-up of 50,007 events + 50,004 links pending (PD-13's per-run ceiling is 100,000), with the default config against the real local alpha.
  - Wall clock 201.7 s: removal 0.04 s, events 181.0 s, links 14.3 s, sync_time 5.5 s.
  - Exit 0; stamped 50,005 / 50,004 (the 2 unstamped events are FU-1).
- **Against the ~250 MB projection:** well inside. P5-19's ≥1 GB task size is generous on this evidence.
- **Not yet measured:** volume at Staging scale (P6-3) and the ledger on disk at volume (P5-3).

*Findings carried forward from §7.2* (details: `ETL-Dgraph-Incremental/README.md` §P4 §7.2, findings 19–25):
- **SIGTERM** ends the port at once with no log line (Go handles it, `main.go:31`) → **P5-13**.
- **An unreachable Postgres** is logged as `startup failed` without a structured `phase` field → candidate for **P5-12 / P5-14**.
- **A run pointed at the fixture template** `parity_fixture` stamps it, and makes every later clone a no-op. The harness now prevents and detects this.
- **`ru_maxrss` of a forked child** includes the parent: use VmHWM.
- **Dumping ~100k nodes in one response** OOM-kills a local alpha in a 5.6 GB Docker VM. That was harness-only; the ETL's run peaked the alpha at 2.2 GiB.
- **The fixture's integer link ids** are not `CHECK`-marked.

**Deviations and findings carried forward** (details: `ETL-Dgraph-Incremental/README.md`, "Findings from P4"):
- **D-1 (open — owner to confirm):** P4-2's "the port asserts it on every mutation" is implemented as *detect and warn, then send as Go does*; failing the run would change Go's behaviour. Tied to the open data question under P4-2.
- **D-2:** the LRU size is a code constant (10,000 entries), not a configuration variable.
- **D-3:** ids are processed in key order, not Go's random map order; the end state is independent of it (§7.0 "Parity consequences").
- **Observed Go behaviours, reproduced and not changed**, not yet classified by the owner (§14.3, O-1 / O-2): duplicate link rows for one `(event1, event2)` leave the facets of whichever row is written last; removal leaves other events' `links` to a removed node dangling.

### P4 gate check — 2026-10-18

Every mandatory gate, re-verified rather than carried forward:

| Gate | Status | Evidence |
|---|---|---|
| **P3 acceptance** (the stated dependency) | **PASS** | §6: all tasks and criteria met; 527 tests pass live. The one literal criterion that cannot be proved without the real secret — live resolution — was **re-scoped by the owner (PD-25) to P5-21**; this is a decision, not an unverified assumption |
| **P1 acceptance** (gates all Python work) | **PASS** | Go `go test ./...` green again today with L3 live; ETL sources unchanged since `b487294`; `ETL-Dgraph-Initial` clean on `main` at `79a38f0` |
| **P0 items P4 consumes:** INV-3 (P4-2), INV-9 (P4-10), INV-5 (P4-11) | **PASS** | INV-3 answered and **corrected** (P0-4); the local alpha reports **v25.3.1**; the INV-5 procedure is documented (P0-6) and the schema applied cleanly to a fresh alpha in the P3 live tests |
| **§3 stop-and-reassess trigger** (INV-8 > 1 M) | **PASS** | Closed by PD-22 — the design no longer depends on the backlog fitting in memory |
| **Owner decisions required by P4** | **PASS** | PD-22 … PD-25 recorded; §14.2 lists nothing outstanding |
| **Local toolchain:** Docker, `python:3.12.3-slim`, `dgraph/standalone:v25.3.1`, the Postgres fixture | **PASS** | All four ran today |
| **Shared fixture invocable from Python without a copy** | **PASS** | `scripts/fixture.sh verify` / `path` / `up`; the lock rejects a changed fixture with exit 3 |
| **Go-parity behaviours recorded as follow-ups, not fixes** | **PASS** | FU-1 … FU-3 (§14.3); P4-19 pins them |
| *Not a gate:* live Secrets Manager resolution | **UNVERIFIED — scheduled** | P5-21 (PD-25). No real secret was read |
| *Not a gate:* `mv_subject_hierarchy` / `master_mill` uniqueness per key | **UNVERIFIED** | Open data question under P4-2; matters for P6 and P4's duplicate-row handling |
| *Not a gate:* ephemeral-storage size of the registered task revision; RDS `idle_in_transaction_session_timeout` | **UNVERIFIED** | P5-19 and P6 respectively (§7.0 limitations) |

**No mandatory gate is FAIL or unverifiable. P4 may start.**

---

## 8. P5 — Verification Harness, Container & Deployment Infrastructure · *Local → Staging*

**Objective.** Build the independent oracle that replaces live differential testing (assessment §4.1), make the container ECS-ready, and stand up the Staging deployment against the provided resources.

**Dependencies.** P4 (acceptance), P0-7, P0-10 … P0-14, and a **populated** `stg/etl-dgraph/creds` (§8.5).

### 8.1 Verification harness *(Local)*

| ID | Task |
|---|---|
| ☑ P5-1 | Build a **reusable** harness: after a run, re-query Postgres for the changeset and assert Dgraph matches — field values, links, facets, `sync_time` stamped **[X] Done 2026-10-19 (Stage 5):** `ETL-Dgraph-Incremental/verify/`, run as `python -m verify verify --changeset FILE --etl-log FILE`.<br>• Read-only on both stores.<br>• Takes the **externally supplied** §9.0.3 changeset and asserts it in full.<br>• A row seeded but never re-NULLed surfaces as a **MISSING EXPECTED ROW** (CS-3) |
| ☑ P5-2 | **[R] Derive assertions from the field mapping, not from the Python implementation** — otherwise it validates the port against itself **[X]** Each assertion's source is tabulated in `docs/HARNESS.md`: `command.txt`, the Go source at `b487294`, a P1 golden, or this plan. A test enforces that `verify/` imports only the secret loader from `src`, and that no derivation cites the port |
| ☑ P5-3 | Instrument the metrics required by assessment §4.3: ID-set and `uidMap` cardinality (now **ledger** row counts), peak RSS, per-phase wall-clock, Dgraph mutation/retry/abort counts — plus, for PD-22, **ingest time and ledger size on disk** **[X]** One `run metrics` line per run, success or failure, carrying all of these.<br>• A run id on every log line: `ETL_RUN_ID`, or random.<br>• Additive instrumentation only; the full suite stays green.<br>• **Not yet measured:** at Staging volume — P6 |
| ☑ P5-4 | Package the harness to run as a standalone Production monitoring check **[X]** A separate process from the ETL, configured with the ETL's variables and secret. `snapshot` captures the pending rows **before** a run, for P7-4 where no owner record exists. `verify/` is inside the image's build context; the image itself is built at P5-7 |
| ☑ P5-5 | Emit a machine-readable per-run verdict (pass/fail + metrics) to stdout — this is the evidence source for PD-1 and PD-2, and must survive being read out of CloudWatch Logs **[X]** One ASCII JSON object per line, carrying the ETL run id, harness run id and changeset id; a final `verdict` line with status and the §4.3 metrics. Round-trip tested through a `filter-log-events` export (out of order) and `aws logs tail` text |
| ☑ P5-6 | **Validate the harness locally first** — run it against the P4-12 local run, and against a deliberately corrupted local graph (negative test) **[X]**<br>• **Against Go:** passes on Go's own end state for all 8 goldens; fails on 8 corruptions of it.<br>• **Against real local runs:** passes on all 8 scenarios.<br>• **Injections, each alone:** wrong value → EV-3; missing link → LK-1; invalidated event present → EV-6; unstamped row → ST-1 |

**P5 §8.1 verification record (2026-10-19, Stage 5).** `ETL-Dgraph-Incremental` at `f73cf7a` plus the uncommitted Stage 5 work.

*Test results*
- **Complete suite, every live service:** **1037 passed, 1 skipped, 1 xfailed**. The skip is P1's own case; the xfail is the P5-13 pin.
- **Offline (`--network none`, no AWS):** default suite 911 passed, guards 36/36.
- **Go repository:** unchanged and green.

*The harness and the port agree on all 7 scenarios plus ALL*, so the ambiguity policy's stop condition was not met. Two harness defects were found and fixed in the harness, never in the ETL:
- a DQL list variable matched as one literal string;
- a CS-4 rule I had inferred, `update ⇒ valid`, which is not in the contract.

*Open items* (`docs/HARNESS.md`): **OI-1** D-1 (indeterminate), **OI-2** O-1, **OI-3** an integral `qty` ≥ 2⁶³ fails the run, Go parity, measured. **OI-4** NaN / ∞ numerics: their graph representation is not in the contract.
- **OI-5** Production column types are unverified.
- **OI-6** Findings print data values; `--hide-values` hashes them, and the default for Staging / Production is an owner choice.
- **OI-7** `snapshot` op is `upsert`.
- **OI-8** clock tolerance.

*Not measured:* the harness's runtime at ~100k records. The volume run was stopped by the host for low memory before it reported. The open question is the restated selections' correlated `EXISTS` queries on unindexed fixture tables.

### 8.2 Container & image *(Local)*

| ID | Task |
|---|---|
| ☑ P5-7 | Dockerfile — **no `.env`** **[V]** (contrast `deployment/dockerfiles/Dockerfile-stg:34`, which `COPY .env`), no tzdata, non-secret config only, pinned dependency versions, non-root user, no build toolchain in the runtime layer **[X] Done 2026-10-19 (Stage 6):** `ETL-Dgraph-Incremental/Dockerfile`, multi-stage.<br>• **No credentials:** inspected layer by layer (`scripts/inspect_image.py`), not asserted. No `.env`, no credential, no secret value. The build itself fails if a credential file reaches `/app`.<br>• **No tzdata:** the base ships it, so it is purged; the bytes stay in the upstream layer.<br>• **No toolchain:** wheels only, no compiler, no pip / setuptools / wheel in the final filesystem.<br>• **Non-root:** uid 10001.<br>• **Pinned:** base by index digest; `requirements.lock` is the full closure, because `requirements.txt` pins only 8 direct dependencies and six transitive ones floated |
| ☑ P5-8 | **Architecture match** — build `linux/amd64` unless INV-10 shows the task definition declares `ARM64`. **[V]** The Go images are `GOARCH=amd64`; a mismatched Python image fails at task start, not at build **[X]** INV-10 (P0 report §4, 2026-10-16) showed `X86_64` for the latest ACTIVE revision `:1`; **built `linux/amd64`**, no discrepancy. Written into every `FROM --platform=linux/amd64`, so a build on an arm64 host cannot quietly differ. Image config `amd64/linux`; `python` reports `x86_64` inside |
| ☑ P5-9 | Drop the meaningless `EXPOSE 8080` **[V]** carried by the Go images — this is a batch task with no listener **[X]** No `EXPOSE`, `HEALTHCHECK` or `VOLUME`; `ExposedPorts` is empty in the image config; a test pins it |
| ☑ P5-10 | **Run the built image locally against the local stack** (§7.2) — same entrypoint, config via env vars, `SECRETS_SOURCE=env`. Confirm full pipeline success and **exit 0** **[X] Done two ways (decision O-6, §8.2):**<br>• **`rehearsal` target, `SECRETS_SOURCE=env`, `.env` mounted at run time:** exit **0**, four phases finished, 11 JSON lines, one run id, empty stderr; the harness from the same image **passed**.<br>• **The exact `deployment` image, `SECRETS_SOURCE` unset (so aws), no `.env`, credentials through boto3 from a local fake Secrets Manager:** exit **0**, same checks, the harness from the same image **passed**, one `GetSecretValue` per process signed for `ap-southeast-3`, no secret value in any output. The deployment image refuses `env` by design (P3-5), which is why the first run uses the sibling target |
| ☑ P5-11 | Repeat the P4-13 failure injections **through the container**; confirm **non-zero exit** and that logs reach stdout before the process dies **[X] Done:** kill → exit 137, alpha stopped → exit 1, Postgres unreachable → exit 1. Each: log lines already on stdout (read after the kill), zero rows stamped, and a retry that exits 0. Details in the §8.2 record |

**P5 §8.2 verification record (2026-10-19, Stage 6).** `ETL-Dgraph-Incremental`: the Dockerfile, `requirements.lock`, `.dockerignore` and the tooling `scripts/image_rehearsal.sh`, `scripts/inspect_image.py`, `scripts/lock_requirements.sh`. Details: `ETL-Dgraph-Incremental/README.md` §P5 §8.2. Local only: nothing was pushed or tagged in any remote registry, and the Go image is untouched.

*Gate.* Stage 4 complete. INV-10 available and unambiguous: `X86_64` (§8.4 V-a). **Caveat:** that dump is from 2026-10-16, so §8.6 step 1 must re-read `cpuArchitecture` from the then-latest revision before P5-20.

*Image.* amd64, uid 10001, 77.6 MB, no EXPOSE / HEALTHCHECK, no tzdata / pip / compiler in the final filesystem. **Build context 198.57 kB** after `.dockerignore`, from a 3.4 MB working tree.

*Reproducibility* (`--no-cache`):
- two directory contexts, mtimes 2026 vs 2001: content-identical over 8,444 entries
- two tar contexts, modes 644 vs 755: content-identical
- the release recipe `SOURCE_DATE_EPOCH=1` + `rewrite-timestamp=true,unpack=false`: **identical image digest**

*Tests.* 28 offline tests of the inspector, 46 offline Dockerfile / lock / `.dockerignore` contract tests, and **8 deliberate Dockerfile mutants, all caught** (EXPOSE added, platform arm64, `--no-deps` dropped, `.env.example` exempted again, root user, `COPY .env`, tzdata purge removed, `SECRETS_SOURCE` baked in). P3's 12-check demonstration still passes on the new image.

*Findings that touch later stages:*
- **P5-13 (SIGTERM).** A Python PID 1 ignores SIGTERM: `docker stop` took the whole grace period and ended in SIGKILL (137); with `--init`, 0.9 s and exit 143. On ECS with the default 30 s `stopTimeout` a stop would wait 30 s unless the process handles SIGTERM or the task definition sets `initProcessEnabled`. Zero rows stamped either way.
- **P5-12.** Config/secret and data/graph failures both exit 1; startup failures log a full traceback in one field.
- **P5-20 / Stage 8.** Register the **deployment** image by digest and check the label `etl.target=deployment`; the `rehearsal` image must never be pushed. A digest-reproducible push needs the release recipe above (`--output type=image,...,push=true`), which does not leave a locally runnable image.
- **`.dockerignore` is not applied to a tar-stream context.** The Dockerfile's own credential check is the last line of defence and now has no exceptions.

**Decision O-6 — [C] resolved by the owner (2026-10-19): option (b).** P5-10 asks to run the image with `SECRETS_SOURCE=env`; the deployment image refuses that by design (P3-5). I added a `rehearsal` build target that differs from `deployment` in exactly one file (`src/build_info.py`, proven by diffing the two filesystems) and ran P5-10 / P5-11 through it. The default target stays the deployment image. The owner chose to **also run the exact deployment image** against a local fake Secrets Manager, entirely local, with synthetic data, fake credentials and no AWS; the evidence is below.

**O-6 (b) verification record** (`scripts/image_rehearsal.sh secrets`, local stack fresh, own database `etl_secrets`):
- **Run as deployed:** `SECRETS_SOURCE` unset (the default, aws), no `.env` mounted, `SECRET_ID` set, region left at its pinned default. Only the endpoint differs from Fargate: `AWS_ENDPOINT_URL` (botocore's own switch) points at `scripts/fake_secretsmanager.py` on a private network. The image's code is untouched.
- **Result:** the deployment image **exited 0**. Four phases finished, 11 JSON lines, one run id, empty stderr, `secrets_source=aws`. Pending rows went from 7 events and 4 links to 2 events (S7, FU-1) and 0 links.
- **On the wire:** exactly **one** `GetSecretValue` per process, for the secret id, signed for **`ap-southeast-3` / `secretsmanager`**. The fake refuses any other region, so the region is the pinned one, not inferred.
- **The harness, from the same deployment image,** passed (430 predicate values over 7 events and 4 links; the run ids join).
- **No secret value** in the ETL's output, the harness's output or the fake's log.
- **Failure paths of the same source, same image,** all exit 1 with no phase started and no value printed: an unknown secret id (`ResourceNotFoundException`), a secret the caller may not read (`AccessDeniedException`), and a secret missing `db_sslmode` (names the key, not a value).
- **What it proves and does not.** The fake is mine. It proves the client side only: the image's boto3, the pinned region, one call per process, the key validation, the reported error codes. It does **not** prove AWS (no signature verification, IAM, rotation or KMS); that stays at **P5-21** against the real secret (PD-25). The fake is itself tested with the real boto3 and the loader's own client (7 offline tests, loopback only).
- **Scope of the failure evidence.** The mid-run injections (kill, alpha stopped, Postgres unreachable) ran through the rehearsal image. On the deployment image the forced failures verified are the credential-path ones above. The code after the credentials is identical in both images.

**Final re-verification** (after the last Dockerfile edits): `build` and `inspect` re-run on the final tree. **Both images pass all 18 layer checks, and the rehearsal image differs from the deployment image in exactly one file, `app/src/build_info.py`.** The consolidated `image_rehearsal.sh all` was stopped by the host for low memory and has not run to completion; every step has been run on its own against the final Dockerfile and passed. Full default suite: **993 passed**.

### 8.3 ECS Fargate task readiness *(this plan's responsibility — PD-4)*

Everything required for the task to run correctly **when triggered**. Workflow placement is excluded by PD-4.

| ID | Area | Task |
|---|---|---|
| ☑ P5-12 | **Exit status** | Exit **0** only when all four phases completed and `sync_time` was stamped. Any unhandled exception, failed secret fetch, unreachable dependency, or partial phase → **non-zero**. **[R]** Distinct codes for *config/secret failure* vs *data/graph failure* so an alarm can tell them apart **[X] Done 2026-10-20 (Stage 7):** `src/exit_codes.py`, table in `docs/ECS_TASK.md` (a test holds them equal).<br>• **0** completed and stamped · **3** config / secret · **4** data / graph · **143** / **130** SIGTERM / SIGINT · **1** unclassified defect · **2** usage.<br>• Each demonstrated on the deployment image; the last log line (`run_end`) carries `exit_code` and `failure`.<br>• A wrong credential *value* is a 4: only Postgres can tell |
| ☑ P5-13 | **Signal handling** | Handle `SIGTERM` (any external task stop — ECS, an operator, or a timeout the owner configures): stop accepting new batches, **never stamp `sync_time`**, exit non-zero promptly. Verify the task definition's `stopTimeout` allows it **[X]** `src/shutdown.py`, installed before the secret fetch.<br>• The signal sets the run's shared `cancel`: no new batch, no further phase, id streams stop at the next row; sync_time is never entered, and a stop *during* it halts at the next 200-row batch.<br>• A 20 s deadline exits by itself, `run_end` written.<br>• **Measured:** deployment image, mid-`events`, `docker stop -t 30` → **exit 143 in 0.5 s**, zero rows stamped. **INV-10 `stopTimeout`: absent → ECS default 30 s.** The proposed revision states 30 explicitly |
| ☑ P5-14 | **Logging** | `awslogs` driver → log group, region `ap-southeast-3`, stream prefix. One JSON object per line to stdout; `PYTHONUNBUFFERED=1`; **no secret value ever logged**. Log explicit phase-start/phase-end markers so clean-cycle criterion **A2** is machine-checkable, and a run identifier correlating the ETL and harness output **[X]** awslogs `/ecs/GAR-Stg-Trace-Task-ETL-Dgraph`, `ap-southeast-3`, prefix `ecs` (INV-10, restated).<br>• A `marker` field on the run and phase lines, plus `seq` on every line so a reordered export can be put back in order.<br>• **`python -m verify phases`** checks A2 from the log alone; its verdict carries the ETL's `run_id`.<br>• Passed on the deployment image, no secret value in any output |
| ☑ P5-15 | **Secrets access** | Runtime `boto3` fetch of `stg/etl-dgraph/creds` from `ap-southeast-3`, **once at startup**, against the **§8.5 key structure**. Validate every required key is present and fail fast with a distinct exit code naming the **missing keys** — never their values. **[R]** Retry transient `GetSecretValue` throttling, not `AccessDenied` **[X]** Deployment image, fake Secrets Manager:<br>• incomplete payload → exit **3** naming `db_schema, db_sslmode`;<br>• `db_port: "fifty"` → 3, value not printed;<br>• AccessDenied → 3 after **1** request;<br>• throttled twice → `Throttling, Throttling, ok`.<br>Retry: standard mode, 5 attempts in total, pinned in code. **Real secret:** resolved on Fargate through the task role at P5-21 (2026-10-20), closing PD-25 |
| ☑ P5-16 | **IAM** | Task-role policy, now fully determined — see §8.5 for the exact document. One statement, one action, one resource ARN; **no `kms:Decrypt`** **[C]** D1. Separate secrets per environment with distinct ARNs, so Staging cannot read Production credentials **[X] Document produced, not applied:** `deploy/staging/iam-task-role-policy.json`, equal to §8.5 (a test compares it with this plan).<br>• **Not a prerequisite gap:** INV-10's task role `SecretManager` already holds `GetSecretValue` through `AdministratorAccess` (D-9), so S4 is moot.<br>• **[C] Owner decision (2026-10-20): keep `SecretManager`.** No dedicated role, and no revision registered for this purpose. The document stays a specification (as D-9). Least privilege is therefore **not** achieved for Staging. **[C] Accepted explicitly by the owner (2026-10-20), `AdministratorAccess` included**; the P5 least-privilege acceptance criterion is waived (M10) |
| ☑ P5-17 | **Configuration** | Non-secret tunables (batch sizes, concurrency, chunk size, log level, **Dgraph alpha endpoints** per §8.5 A3) via the task definition's `environment` block or the baked config file. **No `APP_MODE`** — the Python ETL is single-purpose; carrying the Go two-mode switch forward would be a silent foot-gun. **No replica DB variables** — **[V]** unused (§8.5) **[X]** `deploy/staging/environment.json`, authored fresh.<br>• `DGRAPH_ALPHAS=172.30.0.117:9080` — the Stage 7 brief's `172.30.4.114` is the superseded redis host (INV-14).<br>• `LOGGER_LEVEL=info`; `SECRET_ID`; every tunable at its YAML default (a test holds them equal).<br>• `scripts/task_definition.py check` fails any Go-era, credential, per-run or unknown variable |
| ☑ P5-18 | **Networking** | Confirm from INV-6/INV-10 that the task's subnets and security group reach: Postgres, the Dgraph alphas, Secrets Manager, ECR + ECR-DKR + S3 (image pull), and CloudWatch Logs. **[R]** A private subnet without NAT needs interface endpoints for all of them, not only Secrets Manager — the single most common Fargate task-start failure **[X] Confirmed** (`docs/ECS_TASK.md` §P5-18).<br>• ECR API, ECR DKR, S3 and Logs: endpoints exist; the P5-21 tasks pulled by digest and logged to CloudWatch.<br>• Secrets Manager: endpoint confirmed; the P5-21 `--check` task resolved the real secret.<br>• Dgraph: VPC route and alpha SG ingress on 9080 confirmed.<br>• No new endpoint needed.<br>• **N1 / N2 closed 2026-10-20 [X]** (read-only):<br>&nbsp;&nbsp;– `pg-trace-dev` is in `vpc-0bc87f32d325f2ed2`; both its SGs admit 5432 from `0.0.0.0/0`;<br>&nbsp;&nbsp;– the task SG `sg-008059562cc1c7284` allows all egress.<br>• **Proven at P5-21:** a Fargate task reached Secrets Manager, Postgres and the alpha |
| ☑ P5-19 | **Sizing** | CPU/memory from the §6 projection (~250 MB peak RSS → **[R]** ≥1 GB), refined by P6 measurements; under PD-22 resident memory no longer scales with the backlog (**[X]** the ledger measured +57 MB at the Staging snapshot). **Corrected:** the Python ETL **does** write a file now — the run ledger (§7.0), about 100 MB per 1.3 M ids. Default Fargate ephemeral storage (20 GiB) is ample, but **confirm `ephemeralStorage` in the registered revision** and that the ledger's temp directory is writable **[X] Proposed `cpu 1024` / `memory 2048`** (revision `:1` has 8 vCPU / 16 GB).<br>• Measured peak RSS: 64.9 MB at the 100 k ceiling (Stage 4), ~30× headroom; 2 GiB is Fargate's minimum for 1 vCPU.<br>• CPU unmeasured, so not 0.5 vCPU; P6-7 refines.<br>• `ephemeralStorage` unset (INV-10) = 20 GiB; `/tmp` 1777 and writable by uid 10001 (verified). `check` enforces both |
| ☑ P5-20 | **Task definition revision** | Register a **new revision** of `GAR-Stg-Trace-Task-ETL-Dgraph` **manually, by the §8.6 runbook** **[C]** (PD-6, PD-7): derived from the latest applicable revision, carrying the Python image **by digest**, entrypoint, roles, log configuration and environment. Record the revision number and image digest and hand them to the owner — **no Step Functions change** **[X] Done 2026-10-20:** **revision `:2`**, image **`gar-stg-trace-ecr-dgraph-etl@sha256:87ab74e81134d954b6afc4bb629c123084574cab18a7ee11ad3b85fb152eb37f`** (tag `dgraph-incremental`, built from `ETL-Dgraph-Incremental` `72357a3`).<br>• Derived from `:1`, the latest applicable revision; roles kept (`SecretManager` / `ecsTaskExecutionRole`).<br>• The §8.6 step-5 checklist passes 20 / 20 on the registered revision.<br>• Rollback anchor `deploy/staging/td-go-rollback.json`; `:1` still ACTIVE.<br>• Go `latest` digest unchanged; Go family untouched; Step Functions untouched.<br>• Entry: `ETL-Dgraph-Incremental/deploy/staging/DEPLOYMENT_LOG.md` |
| ◐ P5-21 | **Standalone invocation test** | Run the registered revision via `aws ecs run-task` **directly**, outside any workflow, and verify: task reaches `RUNNING`, secrets resolve, all four phases log, task exits 0, logs land in CloudWatch, and `stoppedReason`/`exitCode` are correct. Repeat with an injected failure and confirm a non-zero `exitCode` surfaces on the task description. **This is the acceptance evidence for PD-4's half of the boundary** **[X] Partly done 2026-10-20** — `run-task` on `GAR-Stg-Trace-Cluster-Platform`, the Go task's network, outside any workflow.<br>• **`--check`** (task `65bb1c4b…`): reached RUNNING; the **real secret resolved** through the task role (`secrets_source: aws`, PD-25 closed); Postgres and Dgraph v25.3.1 reachable; `exitCode` **0**, `stoppedReason` "Essential container in task exited"; logs in CloudWatch.<br>• **Injected failure**, alpha port 1 (task `6677e5a6…`): `exitCode` **4** on the task; CloudWatch names phase `removal` and `failure: data`; nothing written or stamped; peak RSS 75.2 MB.<br>• **[C] Open by owner decision:** the four-phase exit-0 run waits for the PD-13 bootstrap, because it would process and stamp the live Staging backlog and mutate the graph.<br>• **[C] Owner decision (2026-10-20): it is a mandatory gate.** No P6 clean cycle may be declared successful until a four-phase exit-0 run of the registered revision has succeeded on Fargate (§9.2) |

**P5 §8.3 verification record (2026-10-20, Stage 7).** `ETL-Dgraph-Incremental` `c0d34fa` (plus the docstring fix `72357a3`). Details: `docs/ECS_TASK.md` and `README.md` §P5 §8.3. Stage 7 itself was local only: nothing was registered, pushed or applied in AWS. P5-20 and P5-21 followed the same day; see their rows and `deploy/staging/DEPLOYMENT_LOG.md`.

*Gate.* Stage 6 complete. INV-10 has all eleven fields (`stopTimeout` and `ephemeralStorage` absent = defaults). INV-6 answered. ACL not enforced (V-g), so the stop condition was not met.

*Tests.*
- Default suite **1,059 passed**.
- L4 against the local stack **34 passed**; the P4-13 SIGTERM xfail is now a real assertion, and the re-run reaches Go's golden end state.
- `scripts/image_rehearsal.sh ecs` on the deployment image **23 / 23**.

*Handed on, now resolved (2026-10-20).*
- `scripts/task_definition.py derive` and `check` were used at P5-20.
- The image was **rebuilt** from `72357a3` and pushed by digest.
- The decisions listed here are made:
  - the task role: keep `SecretManager` (P5-16);
  - the network: mirror the Go task's launches (P5-21);
  - **N1 / N2** closed by read-only checks and proven by the P5-21 tasks (P5-18).

### 8.4 Staging deployment configuration

**Confirmed — provided and decided by the owner.**

| Item | Value | Source |
|---|---|---|
| Account / region | `389360470396` / `ap-southeast-3` | provided |
| ECR repository | `gar-stg-trace-ecr-dgraph-etl` (existing, **shared**) | **[C]** PD-5 |
| Python image URI | `389360470396.dkr.ecr.ap-southeast-3.amazonaws.com/gar-stg-trace-ecr-dgraph-etl:dgraph-incremental` | **[C]** PD-5 |
| Go rollback image | the existing tag in the same repository — **preserved, never overwritten** | **[C]** PD-5 |
| Task-definition family | `GAR-Stg-Trace-Task-ETL-Dgraph` (baseline revision provided: `:1`) | provided |
| Revision policy | latest applicable revision, identified and verified per §8.6; **Step Functions untouched** | **[C]** PD-6 |
| Registration | manual, per §8.6; no pipeline or IaC automation | **[C]** PD-7 |
| Secret | `stg/etl-dgraph/creds` — **populated** with seven `db_*` keys (§8.5) | **[C]** owner |
| Secret encryption | **AWS-managed KMS key `aws/secretsmanager`** — **[C]** D1, read from the secret's Encryption key field. **No customer-managed key** | **[C]** D1 |
| Secret ARN | `arn:aws:secretsmanager:ap-southeast-3:389360470396:secret:stg/etl-dgraph/creds-0mxXQ4` | **[C]** provided |
| ECR tag availability | `dgraph-incremental` is **not currently in use** in the repository, so it is free for the initial push | **[C]** D3 |
| Postgres grants | The role the ETL authenticates as **holds the privileges tabulated in §8.5** | **[C]** D2 |

**Compatibility findings — the existing artefacts are *not* usable unchanged.**

| # | Finding | Evidence | Consequence |
|---|---|---|---|
| **S1** | The existing image is the **Go** ETL: distroless, static binary entrypoint, `COPY .env` into the image | **[V]** `deployment/dockerfiles/Dockerfile-stg:33` (`COPY .env`) and `:38` (`ENTRYPOINT`) — corrected from `:34,39` (2026-10-16); the binary is named `traceability-dgrapg-etl` (sic, `:22,31,38`). **[A]** The other Dockerfile variants were not re-checked | A new Python image must be built and pushed under `dgraph-incremental`. **Revision `:1` cannot run the Python ETL** — image, entrypoint, roles, log configuration and environment all change. **[X]** Confirmed by INV-10: revision `:1` carries the Go `:latest` with no `entryPoint`/`command`, `environment` or `secrets` |
| **S2** | **CI never pushes `:latest`.** The Azure pipeline tags images `$(Build.BuildId)` only, yet **[C]** the Go ETL runs from `:latest` | **[V]** `azure-pipelines.yml:9-11`, `templates/publish-template.yml` (`docker push …:${ImageTag}`). **[X]** ECR `describe-images` (2026-10-16): 25 images, **all** with numeric CI build-id tags; `latest` is an additional tag on build `165463` | **Resolved for the Python path by PD-5** — the two implementations occupy separate tags, so neither can overwrite the other's artefact. **Residual, recorded not fixed:** **[X]** `latest` is **not** pushed by CI — it is an extra tag on CI build **`165463`** (digest `sha256:730b9c4871583ec9417af817afa96e043104d677cb390538d7ac9c2ee779b465`, pushed 2026-09-30), so the deployed Go image **is traceable to ADO build 165463**. The commit behind that build is **not verified** **[?]** (needs ADO access), and who retagged `latest`, and when, is unknown **[A]**. This is why §8.6 step 2 anchors rollback to the **digest**, not the tag. *(Corrected 2026-10-16: the earlier text said the image had no pipeline provenance — report §3 D-8.)* |
| **S3** | `dgraph-incremental` is a **mutable** tag across Python builds — each push replaces the previous image under the same tag | PD-5 | The tag cannot identify *which* build a Staging cycle ran. **[R]** Record the **image digest** in the cycle log (§9.2) and in the task-definition revision's description; resolve it with `aws ecr describe-images --image-ids imageTag=dgraph-incremental`. This satisfies PD-1's "same artefact across all 5 cycles" requirement without introducing tag automation |
| **S4** | The Go task runs with credentials baked in the image, so its **task role may carry no `secretsmanager:GetSecretValue`** | assessment §7.1 | P5-16 is a **prerequisite**, not a refinement; verify against INV-10 before the first Staging run. **[X] Moot (2026-10-20):** `iam simulate-principal-policy` allows `SecretManager` `GetSecretValue` on the secret, and the P5-21 task read it |
| **S5** | `APP_MODE` and the Go-shaped env block in the existing revision have no meaning for the Python ETL | **[V]** `build.yml` env list | The new revision's `environment` block is authored fresh (P5-17), not inherited |
| **S6** | *Closed.* The secret was empty when §8.5 was written; it is now **populated** with the seven `db_*` keys **[C]**, and §8.5 records the key names as populated rather than as proposed | provided | None outstanding. P5-15 still fails fast on a missing key, by design |

**Requires verification before P5.4.**

| # | Item | Investigation |
|---|---|---|
| ☑ V-a | *Resolved* **[X]** (2026-10-16, report §4 INV-10). Full JSON of the latest ACTIVE revision — `cpu`, `memory`, `runtimePlatform.cpuArchitecture`, `taskRoleArn`, `executionRoleArn`, `networkMode`, `logConfiguration`, `environment`, `secrets`, `ephemeralStorage`, `stopTimeout` | INV-10 (P0-11), via §8.6 step 1 |
| ☑ V-b | *Resolved.* ARN `…:secret:stg/etl-dgraph/creds-0mxXQ4` (§8.4); encryption **[C]** D1, AWS-managed `aws/secretsmanager` | — |
| ☑ V-c | *Resolved.* Secret populated with the seven `db_*` keys (§8.5). **[X]** Verified 2026-10-16: all seven keys present and non-empty, and the credentials authenticated to Postgres (report §5 V-c); the end-to-end task path is first proven at P5-21 | — |
| ☑ V-d | **[X] 2026-10-20: `MUTABLE`** — **ECR tag immutability** on `gar-stg-trace-ecr-dgraph-etl`. **[C]** D3 confirms only that `dgraph-incremental` is **unused**, so the *first* push will succeed either way; it says nothing about the repository's immutability setting. If immutability is **enabled**, the **second** push to that tag fails — which is the first image fix or the P6-7 resize, not the initial deploy. Verify before the second push, not before the first | INV-12 (P0-13) |
| ☑ V-i | **[X] 2026-10-20: no lifecycle policy exists**, so nothing can expire the Go image — **ECR lifecycle policy** on the same repository — confirm it cannot expire the **Go rollback image** (P7-3). Not covered by D3 | INV-12 (P0-13) |
| ☑ V-e | *Resolved* **[X]** (2026-10-16, report §4 INV-6, §5 V-e). The subnets and security group the task runs in, and whether they reach Postgres, Dgraph, Secrets Manager, ECR and CloudWatch Logs. The existing Go ETL's launches (CloudTrail `RunTask`, 14 events) use five subnets — `172.30.0–2.0/24` (IGW route) and `172.30.4–5.0/24` (NAT route) — with `assignPublicIp=ENABLED`; endpoints exist for every dependency. The new family has no launches, so **P5-21 must pick its network configuration from these, with owner agreement** | INV-6 (P0-7) + INV-10 |
| — V-f | *Withdrawn.* **[C]** PD-9 — the family name is the intended reference, and workflow integration is **[O]** owner-owned and out of scope | — |
| ☑ V-g | *Resolved* **[X]** (2026-10-16, report §4 INV-13). Dgraph **ACL** posture on the Staging cluster: an unauthenticated read was **accepted**, so ACL is **not enforced**. **[C]** C2 had proceeded on this as an assumption (§8.5 A4); it is now an observation | INV-13 (P0-14) — **[R]** opportunistic |
| — V-h | *Resolved.* **[C]** PD-8 — `sslmode=disable`; TLS is not in use | — |

### 8.4.1 Runtime configuration inventory — Staging

Closes the gap that §8.4's tables did not cover: **every value the task needs at runtime, where it comes from today, and whether it is known.** Credentials are §8.5; this table is everything else.

| Value | Purpose | Current source (Go) | Status | Blocks |
|---|---|---|---|---|
| **`DGRAPH_ALPHAS`** | Alpha endpoints, comma-separated. **[V]** `app.yaml:35` defaults to `[]` and `dgraph.go:26` **errors immediately** on an empty list — there is no usable default | **[C]** Provided by the owner | **☑ `172.30.0.117:9080`** (corrected 2026-10-16) | — |
| `DGRAPH_SSL_MODE` | **[V]** No-op — all branches use `insecure.NewCredentials()` | `.env` / `app.yaml` | Known; `disable` | — |
| `DGRAPH_TIMEOUT_SECOND`, `DGRAPH_TRANSACTION_TIMEOUT_SECOND`, `DGRAPH_MAX_RETRIES` | Client timeouts and retry ceiling | `app.yaml` defaults (30 / 0 / 100) | Known — defaults usable | — |
| `INCREMENTAL_FETCH_CHUNK_SIZE` and the per-phase batch/concurrency knobs | Throughput tuning | `app.yaml` defaults (1000; batches 10–100, concurrency 4) | Known — defaults usable | — |
| `LOGGER_LEVEL` | Log verbosity | `app.yaml` (`debug`) | Known — **[R]** `info` in Staging | — |
| `DB_CLEAN_*` pool/timeout knobs | Pool sizing, connect timeout, charset | `app.yaml` defaults | Known — defaults usable | — |
| `DB_CLEAN_*` coordinates and credentials | Postgres connection | **[C]** the secret (§8.5), seven `db_*` keys | **Resolved** | — |
| CloudWatch log group | `awslogs` destination | existing Go task definition | **☑ V-a** — group `/ecs/GAR-Stg-Trace-Task-ETL-Dgraph`, driver `awslogs`, prefix `ecs` (INV-10) | P5-14, P5-20 |
| `APP_MODE`, `INITIAL_*`, `DB_CLEAN_REPLICA_*`, `AWS_S3_*`, `DGRAPH_USERNAME`/`PASSWORD`/`BEARER_TOKEN` | — | `.env` | **[V]** Not needed — excluded (§8.5) | — |

**`DGRAPH_ALPHAS` — resolved [C].** Staging value: **`172.30.0.117:9080`** *(corrected 2026-10-16, owner-confirmed: the earlier `172.30.4.114` is the EC2 instance `gar-hara-redis-dev`, not Dgraph; the alpha is `alpha-2026` and its own `/health` reports `172.30.0.117:7080` — report §4 INV-14)*. It is an endpoint, not a credential, and no credential was read to obtain it. Three properties follow, each with a consequence already covered elsewhere in the plan:

| Property | Consequence |
|---|---|
| **A single alpha**, not a list | **[V]** `dgraph.go:51-53` takes the `dgo.NewClient` single-endpoint path; `NewRoundRobinClient` is used only for two or more. The Python client should mirror this — **[R]** accept a comma-separated list in config so a second alpha needs no code change, but expect one today |
| **RFC 1918 private address** | The task's subnet and security group must route to `172.30.0.117:9080` (`172.30.0.0/24` is one of the five task subnets; the VPC's local route covers `172.30.0.0/16` **[X]**) — folded into **V-e** (P5-18). A Fargate task in the wrong subnet fails here, after secrets resolve, which reads as a Dgraph outage rather than a networking error |
| **Plaintext gRPC on 9080** | **[V]** Consistent with `insecure.NewCredentials()` (§8.5) and with `sslmode=disable` (PD-8). No TLS material is needed |

**[R]** Carry the value in the task definition's `environment` (A3) and the deployment log. The same variable in Production will differ and is part of P5-25.

### 8.5 Secrets Manager — specification for `stg/etl-dgraph/creds` **[C] (PD-8)**

**[C]** The secret exists and is **populated by the owner** with the seven `db_*` keys below. This section is the contract between the secret and the loader. **No values appear here** — none were invented and none were read.

**[C] PD-8 settles the two shape questions:** connection coordinates *and* credentials live in the secret, nothing in the task definition; and PostgreSQL uses **`sslmode=disable`**.

> **Where these names come from.** `ETL-Dgraph-Incremental` does not exist yet, so there is no loader to read names from. The **key names** are **[C]** those the owner created in the secret; the **field set and semantics** are derived from the verified configuration contract the Python port must reproduce — the Go `incremental` path's actual reads, cited line by line below. **P3-4 implements the loader against exactly these names.** A later change to a key name changes the secret and this table together, in one step.

#### What the ETL actually needs — derived from the verified Go configuration contract

| Setting | Needed by `incremental`? | Evidence | Secret or config |
|---|---|---|---|
| Postgres host, port, database, user, **password**, schema | **Yes** | `main.go` builds `database.Config` from `cfg.DBClean`; all six are interpolated into the DSN at `pkg/database/postgres.go:11-20` | **Secret** **[C]** PD-8 |
| Postgres pool/timeout tunables (`max_connection_open`, `max_connection_idle`, `max_life_time_ms`, `timeout_second`, `charset`) | Yes | `config/app.yaml` | **Config** — non-secret, task-definition `environment` |
| **Postgres replica block** (`DB_CLEAN_REPLICA_*`) | **No** | **[V]** parsed at `internal/app/config.go:50-60` but **never constructed** — `main.go` builds only the primary connection | **Exclude.** Carrying it forward would create credentials nothing uses |
| Dgraph alpha endpoints | Yes | **[V]** `dgraph.Config.Alphas`, comma-separated via `util.GetEnvAsStringSlice` | **Config** — endpoints are not credentials (see A3) |
| Dgraph `username` / `password` / `bearer_token` | **No** **[C]** per C2 | **[V]** declared in `pkg/dgraph/contract.go:9-12` but **never read** — `NewDgraphConnection` performs no ACL login and no method references them | **Exclude** — see A4, an **unverified assumption** |
| Dgraph `ssl_mode` | — | **[V]** all three branches fall through to `insecure.NewCredentials()` (`pkg/dgraph/dgraph.go:34-43`) — currently a no-op | **Config** |
| Dgraph timeouts / `max_retries` | Yes | `config/app.yaml` | **Config** |
| AWS S3 credentials | **No** | **[V]** `pkg/aws` is imported by no ETL path — dead config | **Exclude** |
| `APP_MODE`, `INITIAL_*` | **No** | Go-only; the Python ETL is single-purpose | **Exclude** |

**[R] Net:** the only genuine secret material the incremental ETL needs today is **one Postgres credential set**.

#### Secret keys — as populated **[C]**

**[C] The owner has populated the secret with these seven keys.** They are now the authoritative contract, and **P3-4's loader is written against exactly these names**:

```json
{
  "db_host":     "<postgres endpoint hostname>",
  "db_port":     "5432",
  "db_name":     "<database name>",
  "db_username": "<etl database user>",
  "db_password": "<that user's password>",
  "db_schema":   "public",
  "db_sslmode":  "disable"
}
```

**All seven keys are required.** The loader treats a missing or empty value as a fatal startup error (P5-15).

| Key | Required | Format | Example | Derivation |
|---|---|---|---|---|
| `db_host` | **Yes** | string — hostname only; **no scheme, no port, no trailing slash** | `my-db.abc123.ap-southeast-3.rds.amazonaws.com` | **[V]** `DB_CLEAN_HOST` → `database.Config.Host` |
| `db_port` | **Yes** | digits; the loader accepts a JSON string **or** number and coerces to int | `"5432"` | **[V]** `DB_CLEAN_PORT` |
| `db_name` | **Yes** | string — database name, not the cluster identifier | `traceability` | **[V]** `DB_CLEAN_NAME` |
| **`db_username`** | **Yes** | string — the database role the ETL authenticates as (**C3**) | `etl_dgraph` | **[V]** `DB_CLEAN_USER` |
| **`db_password`** | **Yes** | string — that role's password (**C3**). The only value that is secret in the strict sense | — | **[V]** `DB_CLEAN_PASSWORD` |
| `db_schema` | **Yes** | string — a single schema name, not a list | `public` | **[V]** passed as `search_path` (`pkg/database/postgres.go:16`). **Load-bearing:** every query in `internal/etl/incremental` names its tables **unqualified**, so a wrong value here fails with "relation does not exist", not a permissions error |
| `db_sslmode` | **Yes** | string — **`disable`** **[C]** PD-8 | `disable` | **[V]** the Go DSN hardcodes `sslmode=disable` (`pkg/database/postgres.go:11`) |

**[R] The loader coerces rather than demands a type.** `db_port` should parse from either `"5432"` or `5432`, because a console edit and an API write can disagree and nobody can tell which was used without reading the secret.

**Deviation from the earlier recommendation, accepted.** The previous revision proposed RDS-convention names (`host`, `port`, `dbname`, `username`, `password`). The `db_`-prefixed names are what exists, so they win — the secret is the source of truth, not this document. One consequence, recorded so it is not rediscovered later: **[X]** AWS-managed RDS rotation writes **unprefixed** `username` / `password`, so those keys would not match if rotation were ever attached to this secret. **[R]** Cheap insurance is for the loader to read `db_username` with a `username` fallback, and likewise for the password — two lines now versus a rename under time pressure later. A recommendation; nothing in the plan depends on it.

**Not in this secret:** Dgraph alpha endpoints (A3 — task-definition `environment`), Dgraph credentials (A4 — unused), replica database settings (**[V]** dead), pool/timeout tunables (non-secret), AWS S3 settings (**[V]** dead), `APP_MODE` (Go-only).

#### PostgreSQL permissions the `db_username` role must hold (C3)

**Documentation only — no user or grant is created or modified by this plan.** Derived from every statement the `incremental` path issues:

| Relation | Access | Where |
|---|---|---|
| `events_bc` | `SELECT`, **`UPDATE`** (`sync_time`) | event queries; `sync_time.go:14` stamps by `unique_id` |
| `links_bc` | `SELECT`, **`UPDATE`** (`sync_time`) | link queries; `sync_time.go:39` stamps by `id` |
| `master_mill` | `SELECT` | `events.go` |
| `subjects` | `SELECT` | `events.go` |
| `mv_subject_hierarchy` | `SELECT` | `events.go` |
| `traceability_log_daily` | **none** | **[V]** referenced only by the removed gate (`traceability_check.go`) — **the Python ETL must not need it**, and a role that still has it is wider than required |
| `mv_events`, `monitoring_cache_shipments`, `mv_shipment_list` | **none** | **[C]** the refresh left the ETL (P4-7); the role needs no privilege on them |

Also required: `USAGE` on the schema named by `db_schema`, and `CONNECT` on `db_name`.

- ☑ **[C] D2 — confirmed by the owner:** the role in `db_username` holds the privileges above. No grant work is required before Staging.
- **[R]** If the role is the existing Go credential set it is a *superset* — it will still carry `traceability_log_daily` and possibly matview privileges, neither of which the Python ETL uses. Narrowing is **optional and safely deferred**; recorded so the wider grant stays a choice rather than an oversight.

#### Assumptions

| # | Status | Assumption | Consequence if wrong |
|---|---|---|---|
| **A1** | **[C] Resolved** (D2) | Whichever role the owner placed in `db_username`, it **holds the privileges above** — confirmed. Whether it is the existing Go credential set or a narrower one is no longer load-bearing | None |
| **A2** | **[C] Confirmed** (PD-8) | Coordinates **and** credentials in the secret; nothing in the task definition | — |
| **A3** | **[R]** | **Dgraph alpha endpoints stay in the task-definition `environment`**, not the secret — they are not credentials, they differ per environment, and keeping them visible makes a misdirected run obvious | Adopted unless the owner says otherwise; moving them into the secret later is a one-line loader change |
| **A4** | **[A] Unverified assumption — explicitly not a confirmed fact** **[C]** per C2 | **Dgraph Staging does not use ACL.** The supporting observation is **[V]** that the Go client connects with `insecure.NewCredentials()` and never logs in, so an enforced ACL would already break the current Go task. That is an inference from the client's behaviour, **not an observation of the cluster**, and no one has checked the cluster | If ACL is in fact enabled, the Python client must perform a login the Go client never did — a **code change in P3-8**, plus `dgraph_username` / `dgraph_password` added to this secret. Cheap in P3; expensive if first discovered at P5-21. Tracked as INV-13 and as a Medium risk (§12). **Update 2026-10-16 [X]:** an unauthenticated read was accepted on the Staging alpha, so ACL is not enforced today (report §4 INV-13); no P3-8 login path or extra secret keys are needed as things stand. *The "[A]" label and the §12 risk row are left for the owner to retire* |
| **A5** | **[C] Confirmed** (PD-8) | PostgreSQL `sslmode=disable` — TLS is not in use. **[V]** This matches the Go implementation exactly, so the port introduces no change in posture. Noted for the record: credentials and row data cross the VPC unencrypted; accepted by decision, revisitable without touching the ETL by changing one secret key | — |
| **A6** | **[A]** | The secret is a **plaintext JSON key/value secret**, not an RDS-managed rotation secret | Only affects whether `engine` / `dbClusterIdentifier` keys also appear; the chosen names are forward-compatible either way |

#### Population and verification (owner action)

- ☑ **[C] Populated by the owner** with the seven `db_*` keys above. **[A]** The *values* are unverified by this plan — nothing here reads them; P5-15 validates presence at startup and P5-21 is the first end-to-end proof that they connect
- ☑ **[C] ARN provided:** `arn:aws:secretsmanager:ap-southeast-3:389360470396:secret:stg/etl-dgraph/creds-0mxXQ4`
- ☑ **[C] D1:** encryption is the **AWS-managed key `aws/secretsmanager`**, so the IAM policy needs `secretsmanager:GetSecretValue` only — no `kms:Decrypt`, no key policy edit

#### Task-role IAM policy (P5-16) — fully determined

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Sid": "ReadDgraphEtlStagingSecret",
    "Effect": "Allow",
    "Action": "secretsmanager:GetSecretValue",
    "Resource": "arn:aws:secretsmanager:ap-southeast-3:389360470396:secret:stg/etl-dgraph/creds-0mxXQ4"
  }]
}
```

Attached to the **task role** (not the execution role) — **[C]** the runtime `boto3` fetch pattern (assessment §7.1). No `kms:Decrypt` statement and no key-policy edit, because the secret uses the AWS-managed key (**[C]** D1). The Production policy is the same document with the Production secret's ARN, which carries a **different six-character suffix** — so the Staging role cannot read it — the separation P5-16 requires.

> **[R] Operational note.** The six-character suffix is assigned at creation. If this secret is ever deleted and recreated — rather than updated — the suffix changes and this policy silently stops matching, surfacing as `AccessDenied` at ETL startup. Either keep the secret and update its value, or treat recreation as requiring a policy update. A `…:secret:stg/etl-dgraph/creds-??????` resource pattern survives recreation and is still far narrower than `*`, if you prefer resilience over exactness.
- ☐ **[R]** Create the Production secret with **identical key names** and a **different ARN** at the same time, so P7 is a configuration swap and the Staging role can never read Production credentials
- ☐ Verify shape with `aws secretsmanager get-secret-value --secret-id stg/etl-dgraph/creds --region ap-southeast-3 --query SecretString`, using the operator's own credentials — **not** from the task
- ☐ P3-4's loader validates all seven `db_*` keys at startup and exits with the §8.3 config/secret failure code naming the **missing keys** (never their values)
- ☐ **[R]** A P3 unit test asserts the loader's required-key list matches this table, so the specification and the code cannot drift silently

### 8.6 Manual task-definition registration & verification **[C] (PD-6, PD-7)**

Manual for now. The procedure changes **only** the task definition — **no Step Functions state, workflow, or IaC is inspected, touched, or reasoned about** at any step **[C]** (PD-4, PD-9). Region is `ap-southeast-3` throughout.

**[C] PD-9 — the reference is the family name** `GAR-Stg-Trace-Task-ETL-Dgraph`, which resolves to the latest `ACTIVE` revision. Two consequences follow, and both are operational, not orchestral:

1. **Registering a revision *is* deploying it.** There is no separate promotion step. So a revision is registered only when deployment is intended — never mid-count during P6 (§9.2 resets the clean-cycle counter on any redeploy), and never as a "draft".
2. **Rollback means registering a new revision**, not re-pointing to an old one, because the family always resolves forward. §8.6 step 2 therefore saves the Go revision's JSON to disk, which turns rollback into a single `register-task-definition` call.

**Step 1 — Identify the latest applicable revision.** The family name without a revision suffix always resolves to the latest `ACTIVE` revision.

```bash
aws ecs list-task-definitions \
  --family-prefix GAR-Stg-Trace-Task-ETL-Dgraph --status ACTIVE --sort DESC --region ap-southeast-3

aws ecs describe-task-definition --task-definition GAR-Stg-Trace-Task-ETL-Dgraph --region ap-southeast-3 \
  --query 'taskDefinition.{revision:revision,image:containerDefinitions[0].image,cpu:cpu,memory:memory,
            arch:runtimePlatform.cpuArchitecture,taskRole:taskRoleArn,execRole:executionRoleArn,
            log:containerDefinitions[0].logConfiguration,env:containerDefinitions[0].environment,
            secrets:containerDefinitions[0].secrets,stopTimeout:containerDefinitions[0].stopTimeout}'
```

Record: the revision number, and **whether its image tag is the Go tag or `dgraph-incremental`**. This answers V-a and establishes the Go baseline for rollback.

**Step 2 — Save the rollback anchor.** Identify the highest revision whose image is the **Go** image, record that image's **digest**, and save the revision's registerable JSON so rollback is one command later:

```bash
aws ecs describe-task-definition --task-definition GAR-Stg-Trace-Task-ETL-Dgraph:<go-revision> \
  --region ap-southeast-3 --query 'taskDefinition' \
| jq 'del(.taskDefinitionArn,.revision,.status,.requiresAttributes,.compatibilities,
          .registeredAt,.registeredBy,.deregisteredAt)' > td-go-rollback.json
```

Commit `td-go-rollback.json` to `ETL-Dgraph-Incremental` alongside the deployment log. This file plus the Go image digest is the P7-2/P7-3 rollback artefact; neither the image nor the revision may be expired or deregistered during the soak.

**Step 3 — Resolve the Python image digest.**

```bash
aws ecr describe-images --repository-name gar-stg-trace-ecr-dgraph-etl \
  --image-ids imageTag=dgraph-incremental --region ap-southeast-3 \
  --query 'imageDetails[0].{digest:imageDigest,pushedAt:imagePushedAt,size:imageSizeInBytes}'
```

**[R]** Reference the image **by digest** (`…/gar-stg-trace-ecr-dgraph-etl@sha256:…`) in the revision, not by tag. The tag is mutable (S3); a digest makes the revision and the cycle log name the same artefact, which is exactly what PD-1 requires. The tag remains the human-facing handle.

**Step 4 — Derive the new revision from the latest one.**

```bash
aws ecs describe-task-definition --task-definition GAR-Stg-Trace-Task-ETL-Dgraph \
  --region ap-southeast-3 --query 'taskDefinition' > td-current.json

jq 'del(.taskDefinitionArn,.revision,.status,.requiresAttributes,.compatibilities,
        .registeredAt,.registeredBy,.deregisteredAt)' td-current.json > td-new.json
```

Then edit `td-new.json`: image digest (step 3), `entryPoint`/`command` for the Python ETL, `taskRoleArn` with the P5-16 policy, `cpu`/`memory` per P5-19, `logConfiguration` per P5-14, a freshly authored `environment` block per P5-17, `runtimePlatform.cpuArchitecture` matching P5-8, and **removal** of every Go-era variable (S5). Leave `secrets` empty — credentials are fetched at runtime by `boto3` (P5-15), not injected.

**Step 5 — Register and verify before any run.**

```bash
aws ecs register-task-definition --cli-input-json file://td-new.json --region ap-southeast-3
```

Pre-deployment verification checklist, run against the **registered** revision with `describe-task-definition`:

- ☐ Image is the digest from step 3, in `gar-stg-trace-ecr-dgraph-etl`
- ☐ `runtimePlatform.cpuArchitecture` matches the architecture the image was built for (P5-8)
- ☐ `taskRoleArn` carries exactly the §8.5 policy document — `secretsmanager:GetSecretValue` on `…:secret:stg/etl-dgraph/creds-0mxXQ4`, and **no `kms:Decrypt`**
- ☐ `executionRoleArn` can pull from ECR and write to the log group
- ☐ `logConfiguration` names an **existing** log group in `ap-southeast-3`
- ☐ `environment` contains no Go-era variable, no `APP_MODE`, and **no credential**
- ☐ `secrets` is absent or empty (runtime fetch, not injection)
- ☐ `cpu`/`memory` ≥ the P5-19 sizing
- ☐ `stopTimeout` allows the P5-13 SIGTERM path to finish

**Step 6 — Hand off, do not orchestrate.** Record the new revision number and give it to the owner. **[C]** PD-9: the intended reference is the **family name**, so the new revision takes effect on the next execution with no further action. **[O]** Whether and how any workflow consumes it is the owner's concern — this plan does not inspect or reason about it. **This plan stops here.**

**Step 7 — Prove it standalone.** Run P5-21 (`aws ecs run-task` against the new revision, outside any workflow) before the revision is used by the workflow.

**Step 8 — Log it.** Append to the deployment log: date, revision number, image digest, Python commit SHA, and the reason. The cycle log (§9.2) cites this entry, so a reset or hold can always be traced to an artefact.

**Rollback by the same procedure:** `aws ecs register-task-definition --cli-input-json file://td-go-rollback.json` (step 2). Because the family resolves forward (PD-9), this re-registers the Go definition as the newest revision; the Go **image** is untouched in ECR throughout (PD-5), so nothing is rebuilt and nothing is overwritten. Verify with the step 5 checklist, then log it.

### 8.7 Deployment infrastructure

| ID | Task |
|---|---|
| ☑ P5-22 | **No new ECR repository for Staging** **[C]** PD-5 — the Python image shares `gar-stg-trace-ecr-dgraph-etl` under the `dgraph-incremental` tag, which **[C]** D3 confirms is **unused**, so the first push is unblocked. Before the **second** push, verify tag immutability is disabled (V-d); before P7-3, verify the lifecycle policy cannot expire the Go image (V-i). **[?]** Whether Production repeats this sharing or uses a dedicated repository is **open** — §15.3 **[X] Staging done 2026-10-20:** the image is in the shared repository as `dgraph-incremental`; V-d `MUTABLE`, V-i no lifecycle policy. The Production question is P5-25 |
| ☑ P5-23 | Apply INV-6: provision the Secrets Manager (and, if needed, ECR/Logs) VPC endpoints **[X] Nothing to provision for Staging:** INV-6 found every endpoint in place, and the P5-21 tasks used them (P5-18). Production is not assessed |
| — P5-24 | **Moved out of P5.** **[C]** PD-10 — pipeline work is deferred until Staging completes; see §15. For Staging the image is built and pushed manually, which P5-21 already proves end to end. The constraints that will apply whenever the pipeline is built are recorded in §15.5 |
| ☐ P5-25 | Mirror the configuration for Production **before** P7 — Production secret with identical keys and a distinct ARN, and the equivalent image tag in the **Production account's** registry (**[C]** PD-11: same tag convention, separate accounts, §15.4) — so cutover is a task-definition swap and not a first-time build. **[?]** Which Production repository holds it is open (F4, §15.3) |

**P5 acceptance — [C] conditionally accepted (2026-10-20).** Every criterion below is met, except the IAM criterion, which the owner waived. The conditions stay in force:
1. **M9** — the P5-21 four-phase exit-0 run must succeed before any P6 clean cycle is declared (§9.2);
2. **M10** — the IAM waiver above;
3. outside acceptance and still open: P5-21's four-phase run (◐), and P5-25, due before P7.

### Deliverables
- Verification harness, runnable standalone, emitting machine-readable verdicts
- Python image, locally proven against the local stack, published as `gar-stg-trace-ecr-dgraph-etl:dgraph-incremental`
- New Staging task-definition revision, registered manually per §8.6 and proven by a standalone `run-task`
- Secret structure specification (§8.5), confirmed and populated by the owner
- IAM roles, networking, deployment log

### Acceptance criteria
- ☑ Harness detects a **deliberately injected** graph discrepancy (negative test — proves it is not vacuously passing), verified locally first **[X]** 2026-10-19: four separate injections on real local runs, plus eight corruptions of Go's golden end state (P5-6)
- ☑ Image builds and runs the full pipeline **locally** against the local stack, exit 0 **[X]** 2026-10-19: the deployment image, run as deployed, exit 0 (O-6 b, §8.2). **Local half only:** the Fargate half is P5-21
- ☑ Image runs on Fargate via standalone `run-task` (P5-21); secrets resolve from the populated `stg/etl-dgraph/creds`; **no credentials in the image** **[X]** 2026-10-20, as worded here: it runs on Fargate (`--check` exit 0; the pipeline through `removal` in the failure run), the real secret resolves, and the layer inspection passed on the pushed build. **The P5-21 task row's four-phase run is not part of this criterion; it is carried as a mandatory gate before any P6 clean cycle (owner decision, §9.2)**
- ☑ An injected failure surfaces as a non-zero `exitCode` on the stopped task, with the failing phase named in CloudWatch Logs **[X]** 2026-10-20: `exitCode` 4, phase `removal` named (P5-21)
- ☑ A missing or malformed secret key fails at startup with the config/secret exit code and names the missing key, not its value **[X]** 2026-10-20, deployment image against the local fake: exit **3**, `missing or empty: db_schema, db_sslmode` / `invalid: db_port`, no value printed (§8.3 P5-15). Against the real secret: P5-21
- ☑ The Go image and its last Go-based task-definition revision are **both still intact** after the Python image is published (PD-5, §8.6 step 2) **[X]** 2026-10-20: `latest` still `sha256:730b9c48…`; `:1` ACTIVE and saved as `td-go-rollback.json`; `GAR-Stg-Trace-task-dgraph-etl:1` unchanged
- ☑ The §8.6 pre-deployment checklist passes against the registered revision, and the deployment log entry exists **[X]** 2026-10-20: 20 / 20; `deploy/staging/DEPLOYMENT_LOG.md`
- ☑ ~~IAM verified least-privilege: the Staging role **cannot** read the Production secret~~ **[C] Waived by the owner (2026-10-20, M10)**: the Staging task role stays `SecretManager` with `AdministratorAccess`. **Implications, accepted:**
  - the Staging task can read **every** secret and resource in account 389360470396, not just `stg/etl-dgraph/creds`;
  - "cannot read the Production secret" now rests on the **account boundary alone** (PD-11). A secret under the AWS-managed key `aws/secretsmanager` cannot be read from another account, and none has a cross-account resource policy. **Unverified until P5-25 creates that secret**;
  - the §8.5 policy stays a specification, ready for P5-25 / P7, where the Production task role is decided **afresh**: this waiver is for Staging only.
- ☑ §8.4's **V-a, V-b, V-c, V-e, V-g** resolved (V-d and V-i are later gates, per §14.2 E2/E3) **[X]** all five ☑ in §8.4, and V-d / V-i too (2026-10-20)

---

## 9. P6 — Staging Validation · *Staging*

**Objective.** Validate integration **and scale** on Staging, which **[C]** is representative of Production.

**Dependencies.** P5 (acceptance), Local exit criteria (§2.1) recorded as met, and **INV-7 — due here, having been deferred through Local** (P0-8, §9.0.4). **[C]** PD-14 — **no Step Functions dependency.**

**Exit threshold (PD-1, PD-13) [C]:** the §9.0 bootstrap, then **5 consecutive clean cycles, Day 1 … Day 5.**

### 9.0 Staging validation sequence **[C] (PD-12, PD-13)**

**[C]** Staging data is updated **less frequently** than Production, so the five cycles are **owner-seeded**. Representativeness comes from the **content and per-run volume** of what is seeded, not from the cadence.

**Step 0 — Initial ETL, once.** Bootstrap the Staging graph from the Staging dataset before any incremental run.

| ID | Task |
|---|---|
| ☐ P6-0a | Run the **Go `initial` mode** against Staging Postgres (`APP_MODE=initial`). **[V]** It needs **no Dgraph connection** — it streams Postgres and writes gzipped RDF to `initial_imports/`, and **[V]** deletes that directory wholesale at the start of every run |
| ☐ P6-0b | Apply `schema.dql` (**INV-5**) and load the exported RDF with **`dgraph bulk`** **[C]** PD-19, per the §9.0.4 procedure — stop alphas, bulk, distribute `p`, restart, verify |
| ☐ P6-0c | **Stamp the baseline** **[C]** PD-18 — `sync_time = NOW()` for the processed rows still `NULL`, **after** the load is verified and **before** any Day-1 seeding. Scope and ordering: §9.0.1 |
| ☐ P6-0d | **Confirm exclusive execution** (**[C]** PD-16): no incremental run starts until the bootstrap and the §9.0.1 check are complete. An operational assumption, held by sequencing — **[V]** no code enforces it |
| ☐ P6-0e | Record the post-bootstrap graph state as the **baseline** for the harness: node and edge counts, and the `sync_time IS NULL` row count (expected: zero, or only deliberately seeded rows) |

#### 9.0.1 Watermark semantics and post-initialization stamping **[C] (PD-15, PD-18, PD-20)**

**What the watermark means.** **[V]** Every incremental source query filters on `sync_time IS NULL` (`events.go:16,34`, `links.go`, `removal.go`), and **[V]** `sync_time` is only ever written **forward** to `NOW()`, in `sync_time.go`, after the graph mutations succeed.

**[C] Decision 1:** `sync_time = NULL` identifies **new or updated source records**, and those are exactly the records the Incremental ETL must process.

| Row | Required `sync_time` | Why |
|---|---|---|
| **New or updated** in `events_bc` / `links_bc` | **`NULL`** | The Incremental ETL selects on it |
| **Already loaded by a verified Initial ETL** | **not `NULL`** (PD-18) | So Day 1 … Day 5 process **only post-Initial changes, never the full dataset** |

**[C] PD-20 — re-nulling is external and out of scope.** Returning `sync_time` to `NULL` when a source record is inserted or updated is handled outside the Dgraph ETL. This plan does not investigate, document, or depend on how. The boundary is clean: **the ETL consumes `NULL`; producing it belongs to the source system.** `INV-16` is withdrawn accordingly.

One consequence worth a single line, requiring nothing of that external process: because the harness asserts against the **owner-supplied changeset** (PD-17, §9.0.3), a record that was seeded but never marked `NULL` surfaces as a **missing expected row** rather than passing unnoticed. Validation is self-protecting here.

##### Post-initialization stamping **[C] (PD-18)**

**[V]** `initial` mode never writes `sync_time` — no reference anywhere in `internal/etl/initial`, and its queries filter on `status = 'valid'` alone. Without this step every row stays `NULL` and the first incremental run would select the **entire** dataset, which decision 2 forbids and which at scale would breach the §6.1 **>1M** guardrail and criterion **C3** by construction.

**Statement:**

```sql
UPDATE events_bc SET sync_time = NOW() WHERE sync_time IS NULL;
UPDATE links_bc  SET sync_time = NOW() WHERE sync_time IS NULL;
```

**Scope — exactly the rows the Initial ETL processed, and no others:**

| In scope | Out of scope |
|---|---|
| `events_bc` and `links_bc` rows the Initial export emitted | Any other table |
| Rows whose `sync_time` is still `NULL` at the moment of stamping | Rows already stamped — the `WHERE` clause excludes them, so the statement is idempotent and re-runnable |
| **[V]** The export selects on `status = 'valid'`, so invalid rows were never exported | **[?]** Whether invalid rows should also be stamped. **[R]** Leave them `NULL` — the removal path (`removalQuery`) selects invalid rows with `sync_time IS NULL`, so stamping them would hide genuine deletions from Day 1 |

**Ordering — three constraints, each load-bearing:**

1. **After the Initial ETL run completes successfully.** A partial export followed by a stamp marks rows as synced that were never exported.
2. **After the load output is verified** (**[C]** PD-18) — the bulk load finished and the graph is queryable, per P6-0e's baseline counts. **[V]** Nothing ever returns `sync_time` to `NULL` inside this codebase, so a premature stamp is not self-correcting: those rows would be permanently invisible to the Incremental ETL while absent from the graph.
3. **Before any Day-1 data is seeded.** Stamping after seeding marks the Day-1 changeset as already synced, and Day 1 then validates nothing.

**[C] Performed manually** as a §9.0 runbook step. The Go `initial` path is feature-frozen and is not modified for this.

#### 9.0.2 Execution exclusivity — operational assumption **[C] (PD-16)**

**[C]** Initial and Incremental **never run concurrently.** Initial runs **once** for the initial load, or is **rerun when a major change requires refreshing all data in Dgraph**.

This is an **operational assumption held by sequencing**, not a mechanism: **[V]** no code enforces exclusion, and **[C]** PD-14 keeps orchestration out of scope, so no guard is built here. Assessment §2.1 is the basis — `initial` rebuilds the graph wholesale while `incremental` mutates it in place.

**[R]** Whenever Initial is rerun — in Staging or later — the §9.0.1 sequence applies again in full: bulk load, then the watermark check, then incremental may resume. A rerun that skips the watermark step leaves stamped rows pointing at a graph that no longer contains them.

#### 9.0.3 Steps 1–5 — Incremental ETL, Day 1 … Day 5

Each day, in order:

1. **[O] Owner seeds and records the changeset** — manual inserts and/or updates to source data. **[C]** PD-17: the owner **identifies and records which records were inserted or updated**; this plan does not determine or generate it. **[C]** Volume varies by day, **below 100,000 records**.
2. Run the Python incremental ETL (**[C]** PD-14 — however the owner triggers it is not this plan's concern).
3. Run the verification harness (P5-1) **against the owner-supplied changeset record** and capture the §4.3 metrics.
4. Record the cycle verdict, counter state, image digest and revision in the cycle log.

**What the harness needs from the changeset record** — enough to assert without inferring, and no more:

| Field | Purpose |
|---|---|
| `events_bc.unique_id` per affected event, with the operation (insert / update / invalidate) | Drives the B1–B4 assertions on field values and on absence after invalidation |
| `links_bc.id` and its `event1_id` / `event2_id` per affected link, with the operation | Drives link-presence and facet assertions; **[V]** `event1_id` is what link chunking keys on |
| Row counts per table | Cross-checks against the C3 cardinality metric |

**[R] This is a materially stronger oracle than sampling.** Because the expected set is supplied rather than derived, the harness compares against an **independent** statement of what should have changed — which is exactly the property P5-2 asks for, and one that Staging could not otherwise provide. Criterion **B1** is written against the complete supplied set. A cycle whose changeset was not recorded cannot be adjudicated and is **held, not failed** (§9.2).

#### 9.0.4 Loader — `dgraph bulk` **[C] (PD-19)**

Selected on repository evidence and the provisioned cluster. **[C]** `dgraph live` is not adopted; it would be a different approach, and nothing in the evidence justifies one.

**Evidence for `dgraph bulk`:**

| Evidence | Bearing |
|---|---|
| **[V]** `command.txt:3` — *"first initial migration **with bulk loader**… the scope of work initial migration is just until create the rdf files"* | The stated intent, and the reason the ETL stops at file creation |
| **[V]** `main.go` — `initial` mode never constructs a Dgraph client | The ETL cannot load anything itself, by design; the loader is out of band |
| **[V]** `aggregator.go:19`, `links.go:46` — gzipped N-Quad chunks in `initial_imports/` | The native input format for `dgraph bulk` |
| **[V]** `links.rdf.gz` references `_:event_<id>` defined in `events.rdf.gz` | The two files must resolve blank nodes in **one** load. Bulk does this naturally in a single run |
| **[V]** `setup_dgraph_alpha.sh:100,117` — alpha runs `--postings=/data/alpha/p` | A filesystem `p` directory, which is what bulk produces |
| **[V]** `setup_dgraph_alpha.sh:150-153` — `rsync` of `/data/alpha/p/` to a peer alpha | The `p`-directory distribution step, already tooled in this repository |
| **[V]** `setup_dgraph_zero.sh:101-107` — three zeros, `--replicas=3` | A replicated cluster: bulk's output is copied to each replica in a group, which is exactly what the rsync does |
| Scale — millions of rows | Bulk is materially faster than issuing mutations |

**[V]** `schema.dql:1` declares `unique_id: string @index(exact, hash, trigram) @upsert`, and `aggregator.go:77` emits `<unique_id>` for every event — so after the load the Incremental ETL resolves uids by `unique_id`, independent of blank-node labels. The loader choice does not affect anything downstream.

**Procedure skeleton — the shape is evidence-based; the marked values are not yet confirmed.**

1. **Stop the alphas.** **[X]** `dgraph bulk` writes a fresh `p` directory and requires an empty cluster; it cannot run against alphas holding data. For Day 0 this is intended — the load *is* the bootstrap.
2. **Run the bulk load** against the zeros, pointing at `initial_imports/` and `schema.dql`:
   `dgraph bulk --files initial_imports --schema schema.dql --zero <zero>:5080 --out <dir>` — **[?]** exact `--zero` target, `--reduce_shards` and `--map_shards` for a `--replicas=3` cluster.
3. **Place the output** — the produced `p` becomes each alpha's `/data/alpha/p`, distributed by the existing `rsync` pattern (`setup_dgraph_alpha.sh:150-153`), one copy per replica in the group.
4. **Start the alphas** and confirm health, then record P6-0e's baseline counts.
5. **Only then** apply the §9.0.1 stamping.

**INV-7 — [X] partly resolved 2026-10-20** (owner-authorised, read-only on the cluster; local rehearsal). Record: `INV7_BULK_LOAD.md` in `deploy/staging/` of `ETL-Dgraph-Incremental`.
- **Live topology:** **one zero, one alpha, one group**, not `--replicas=3`.
- **State:** **holds 28.8 GB** (99.9 GB uncompressed), `maxUID` 35.3 M — the load replaces it.
- **Invocation:** `dgraph bulk … --zero 172.30.0.85:5080 --map_shards 1 --reduce_shards 1`, against a **fresh zero** (existing `p` / `w` / `zw` moved aside), so there is no `rsync` step.
- **Rehearsed locally on v25.3.1:** data replaced; counts, facets and the schema verified; the Python Incremental resolves the bulk-loaded nodes with zero duplicates.
- **A resource scheduler stops both hosts 12:09–23:09 UTC on weekdays.**
- **Open before P6-0b:** H1–H6 in that record — host process and paths, the arm64 binary and disk, resources at scale, the scheduler window, a rollback snapshot, and moving the RDF.

*Original text, kept:* **[?] Open — narrow and operational, tracked as INV-7. [C] DEFERRED: not investigated or executed now.** Taken up when **Local validation is complete and Staging implementation is about to begin**; **mandatory before P6-0b**. The items below are preserved unchanged and none of them blocks Local work:

- The exact `dgraph bulk` invocation: `--zero` target, shard counts, output path.
- Whether the Staging cluster is **empty** at bootstrap, or holds data the load will replace. For Day 0 replacement is expected; it needs stating, not avoiding.
- **[?]** Whether the cluster these scripts provision is the one the ETL targets. **[V]** The scripts describe alphas and zeros on `172.30.0.x`, while **[C]** the Staging ETL endpoint is `172.30.0.117:9080` (§8.4.1), in the same `172.30.0.0/24` subnet. **[X]** None of the scripts' addresses (`172.30.0.36/.65/.95/.171`) exist any more — the cluster was rebuilt as `alpha-2026` / `zero-2026` (2026-10-16). The scripts document an earlier cluster, so **confirm the live topology before relying on their addresses** (still INV-7, deferred; not otherwise investigated).

### 9.1 Clean-cycle definition **[C]**

A cycle counts as **clean** only when **all three dimensions pass**. Any single failure disqualifies the cycle.

| Dim. | Criterion | Measurement |
|---|---|---|
| **A1** | ECS task **exits 0** and is not stopped externally | Task `exitCode` / `stoppedReason` |
| **A2** | All four phases completed: removal → events → links → sync_time | Phase-completion log (P5-14 markers) |
| **A3** | Completed on the **first invocation** — no re-run required | Invocation count = 1 |
| **B1** | Verification harness passes with **zero** discrepancies across the **complete owner-supplied changeset** — not a sample **[C]** PD-13, PD-17 | P5-5 verdict vs. the §9.0.3 changeset record |
| **B2** | No `sync_time IS NULL` residue for rows in the processed changeset | Post-run query |
| **B3** | Every valid link in the changeset present in the graph with correct facets | Harness assertion |
| **B4** | Every invalidated event in the changeset **absent** from the graph | Harness assertion |
| **C1** | Peak RSS within the assessment §6 envelope; no OOM | Container metrics |
| **C2** | Wall-clock within the band set by the first clean cycle — **[R]** no more than a 50% regression, normalised for that day's seeded volume | Run duration vs. seeded record count |
| **C3** | ID cardinality consistent with the day's seeded volume (**[C]** <100k records) and the §6 amplification; **not** >1M | P5-3 metric |
| **C4** | Dgraph retry/abort counts within the baseline from the first clean run | P5-3 metric |
| **C5** | Zero unhandled exceptions or `ERROR`-level logs | Log scan |

### 9.2 Counter rules **[C]**

The distinction that matters: a **qualifying failure** indicates a defect in the Python ETL and **resets** the counter; a **non-qualifying** event means no valid observation occurred, so the counter **holds** and the cycle is simply excluded.

| Event | Counter effect |
|---|---|
| ETL task failure in any phase | **Reset to 0** |
| Parity/correctness mismatch (B1–B4), unresolved | **Reset to 0** |
| Operational-health breach (C1–C5) | **Reset to 0** |
| Run succeeded only after a **re-run**, root cause **in the ETL** | **Reset to 0** |
| **Any code change to the Python ETL** | **Reset to 0** — prior cycles no longer evidence the deployed artifact |
| **Any new image or task-definition revision deployed to Staging** | **Reset to 0** — the artefact under observation changed. **[R]** Because `dgraph-incremental` is a mutable tag (§8.4 S3), the comparison is made on the **image digest** recorded in the §8.6 deployment log, not the tag |
| Mismatch traced to a **harness defect**, ETL proven correct | **Hold**; cycle excluded; re-verify after the harness fix |
| Harness verdict **`indeterminate`** whose **only** non-pass findings are **EV-5 for D-1**: duplicate `eventDetailsQuery` rows that disagree on a field (the finding's text names D-1). **[C]** Option A, owner, 2026-10-20 | **Hold**; cycle excluded. **Not** a reset: the ETL behaved as Go, and under Option A that is the specification. **Not** clean either: B1 requires a `pass`, and the affected fields have no single expected value. Record the events and fields in the cycle log. The WARN line `mutation sets and deletes the same field` is the ETL's side of the same signal; it does not breach C5, which counts ERROR lines |
| Harness verdict **`indeterminate`** from **anything else**: CS-2 (several `events_bc` rows per `unique_id`), EV-5 / LK-2 for a non-finite numeric (OI-4) | **Hold until classified**: an ETL defect → **reset**; a harness defect → hold, as above; a data condition outside the contract → owner decision |
| Run succeeded after a re-run, root cause **external and transient** (verified) | **Hold**; cycle excluded |
| No run occurred on a given day — no data seeded, or the owner did not trigger it | **Hold**; cycle excluded. **[C]** PD-12: cycles are seeded, not scheduled, so a gap is an absence of observation, not a defect |
| The day's changeset was **not recorded** by the owner, so B1 cannot be adjudicated against it | **Hold**; cycle excluded **[C]** PD-17 |
| **The P5-21 four-phase exit-0 run has not yet succeeded** on the registered revision **[C]** (owner, 2026-10-20) | **Hold**; no cycle can be declared clean until it has, whatever its verdict |
| **Day 0 — the Initial ETL and its load** (§9.0) | **Excluded by definition** **[C]** decision 2 — Day 0 is the bootstrap itself, not an incremental cycle. Measured in its own right; not one of the five |
| External platform outage prevented the run | **Hold**; cycle excluded. **[R]** Investigate if it recurs |

**[R]** Record every cycle's verdict, counter state, **and the image digest it ran** in a run log, so the exit decision rests on evidence rather than recollection.

### 9.3 Tasks

| ID | Task |
|---|---|
| ☐ P6-1 | Deploy the Python revision registered in P5-20 (§8.6). **[C]** The Go image stays in ECR untouched (PD-5) and `td-go-rollback.json` (§8.6 step 2) is committed as the rollback anchor. **[C]** PD-9: the family name resolves to this revision on the next execution |
| ☐ P6-2 | Run the five cycles per §9.0 — owner seeds, ETL runs, harness verifies, log updated |
| ☐ P6-3 | Capture §4.3 metrics every run; compare against §6 projections **and against the local baseline from §2.1** |
| ☐ P6-4 | Record per-phase and total wall-clock for every run and **hand the figures to the owner** for whatever timeout they choose to set. **[C]** PD-14 — this plan sets no timeout and is not gated by one |
| ☐ P6-5 | Exercise re-run safety: force a mid-run failure, then re-invoke and confirm clean reprocessing with **zero rows stamped** by the failed attempt. **[R]** Run this deliberately **after the §9.0 bootstrap and before Day 1**, so the test does not reset the count |
| ☐ P6-6 | **[R] Optional** `sync_time` replay check (assessment §4.2). **Note the documented limitation:** the removal path cannot be cleanly A/B'd this way — covered by P1-5 and scenario 3 instead |
| ☐ P6-7 | Right-size the Fargate task from measured peak RSS; register the resized revision **before** the clean-cycle count begins, not during it (§8.6) |
| ☐ P6-8 | Maintain the cycle log (§9.2) |

### 9.4 Acceptance criteria

- ☐ The §9.0 bootstrap completed: Initial ETL run, schema applied, load verified, and the **baseline stamped** so Day 1 processes only post-Initial changes (P6-0c, PD-15)
- ☐ **5 consecutive clean cycles, Day 1 … Day 5** **[C]** (PD-1, PD-13), every dimension in §9.1 passing, all on the **same image digest and task-definition revision**. **[C]** Day 0 is the Initial ETL and is excluded (decision 2)
- ☐ Each day's changeset **recorded by the owner** (**[C]** PD-17) and used by the harness as its **expected result**, asserted in full rather than sampled
- ☐ **No cycle processed the full dataset** — each day's selected ID count is consistent with that day's recorded changeset (**[C]** decision 2)
- ☐ Cycle log complete, each entry with verdict, metrics, image digest, revision number, and counter state
- ☐ Task sizing set from measured values, not estimates; durations recorded and handed to the owner (P6-4)
- ☐ Retry behaviour demonstrated (P6-5) and documented
- ☐ Zero open defects classified Critical or Major (§10.3)

---

## 10. P7 — Production Cutover & Decommission · *Production*

**Objective.** Cut Production over, soak, then retire the Go incremental path.

**Dependencies.** P6 (acceptance), Staging exit criteria (§2.2) recorded as met.

### 10.1 Cutover tasks

| ID | Task |
|---|---|
| ☐ P7-1 | Register the Production task-definition revision with the Python image, **manually per §8.6** **[C]** (PD-7), and hand the revision number to the owner; **[O]** the owner performs any state change |
| ☐ P7-2 | **Retain the Go image as the rollback artifact** **[C]** (PD-5) — a separate tag, never overwritten by a Python push. Its self-gate is a harmless no-op if re-engaged |
| ☐ P7-3 | **Exempt the Go image and the last Go-based task-definition revision from ECR lifecycle expiry and deregistration** for the soak duration. Record the **digest**, not only the tag (§8.4 S3, §8.6 step 2) |
| ☐ P7-4 | Run the verification harness after every Production run during the soak |
| ☐ P7-5 | **[R]** Monitor via the `sync_time` watermark — a silent failure leaves rows unstamped, detectable by counting `WHERE sync_time IS NULL` before and after |
| ☐ P7-6 | Maintain a soak log mirroring §9.2, with severity classification per §10.3 |

### 10.2 Soak period **[C] (PD-2)**

**Minimum 14 consecutive calendar days** of Production operation with the Go implementation retained and deployable.

- The soak clock starts on the **first Production run** after cutover.
- The clock is measured in **calendar days**, so days on which no run occurs (e.g. an upstream halt per assessment decision 11) still elapse.
- **[R] Supplementary safeguard:** require **≥10 successful Production cycles** within the soak window. 14 calendar days containing few actual runs would satisfy the letter of PD-2 while proving little. Flagged as a recommendation, not a confirmed decision — drop it if the workflow's cadence makes it redundant.

### 10.3 How significant failures affect the soak **[C]**

| Severity | Definition | Effect on soak |
|---|---|---|
| **Critical** | Data corruption or silent divergence; data loss; manual data repair required; rollback invoked; any defect that would have corrupted the graph undetected | **Soak restarts at day 0**, beginning after the fix is deployed **and** one clean cycle is observed |
| **Major** | Run failure requiring intervention but no data impact; performance regression beyond the §6 envelope; a harness failure revealing a real ETL defect with contained impact | **Soak extends by 7 calendar days** from the date the fix is deployed |
| **Minor** | Logging, cosmetic, or non-impacting; an external transient resolved by automatic retry | **No extension**; tracked to closure |

Additional rules:

- **Rollback invoked at any point** → soak restarts at day 0 after re-cutover.
- **Any functional code change** to the Python ETL during the soak → treated as at least **Major** (7-day extension from deployment), because prior soak days no longer evidence the deployed artifact.
- **Non-functional changes** (logging, metrics, documentation) → no extension, recorded in the soak log.

### 10.4 Decommission gate **[C]**

**All four conditions must hold** before any task in §10.5 begins:

- ☐ **G1 — Soak complete:** ≥14 calendar days elapsed per §10.2, including any extension or restart from §10.3
- ☐ **G2 — No open Critical or Major issues**
- ☐ **G3 — Validation criteria met:** the verification harness passed on every Production run in the final 14 days; **[R]** ≥10 successful cycles within the window
- ☐ **G4 — Rollback no longer required:** explicit recorded sign-off, with a named approver and date

### 10.5 Decommission tasks *(gated on §10.4)*

| ID | Repo | Task |
|---|---|---|
| ☐ P7-7 | Initial | Remove `internal/etl/incremental` and `pkg/dgraph` **[V]** (imported only by incremental/runner/main) |
| ☐ P7-8 | Initial | Simplify `main.go`, `runner.go`, `mode.go` — drop the two-mode dispatch |
| ☐ P7-9 | Initial | **Delete the non-canonical `schema.dql` copy** and the five `COPY schema.dql` lines **[V]** (assessment §2.3 I1) |
| ☐ P7-10 | Ops | Update the bulk-load runbook to fetch canonical `schema.dql` from `ETL-Dgraph-Incremental` |
| ☐ P7-11 | Ops | **Add a predicate-drift check before any bulk reload** (assessment §2.3 I2): compare predicates emitted by the frozen Go exporter against the canonical schema |
| ☐ P7-12 | Initial | Declare the repository feature-frozen; document it in the README |
| ☐ P7-13 | Ops | Release the ECR lifecycle exemption from P7-3 |

### 10.6 Acceptance criteria

- ☐ G1–G4 all satisfied and recorded
- ☐ Rollback rehearsed **before** P7-7 — the Go image redeploys and runs successfully
- ☐ Post-decommission: `ETL-Dgraph-Initial` builds, its `initial` path runs, and it contains **no Dgraph client**
- ☐ Bulk-load runbook and drift check documented and exercised once

> **[R] Decommission is irreversible for rollback purposes.** §10.5 must not begin until §10.4 is fully satisfied.

---

## 11. Investigation register

Open technical details carried as tasks, never assumptions.

| ID | Item | Phase | Blocks if unanswered |
|---|---|---|---|
| ~~**INV-1**~~ | ~~Step Functions `.sync` integration pattern~~ | — | **[C] Withdrawn (PD-14)** — out of scope; blocks nothing |
| ~~**INV-2**~~ | ~~Halt-on-failure, retry, timeout, concurrency guard, alarms~~ | — | **[C] Withdrawn (PD-14)** — out of scope; blocks nothing |
| **INV-3** | pydgraph `set_nquads` + `del_nquads` in one mutation. **Corrected 2026-10-18:** yes, but a *same-predicate* set+delete is undefined (P0-4) | P0-4 | P4-2 set-or-delete per field — a hard requirement |
| **INV-4** | pydgraph gRPC gzip via channel options | P0-5 | P3-8 (performance only, not correctness) |
| **INV-5** | Schema-application procedure; deployed-vs-file match | P0-6 | P3-2, P4-11, P7-10 |
| **INV-6** | VPC endpoint requirements (Secrets Manager, ECR, Logs) | P0-7 | P5-18, P5-23 |
| **INV-7** **◐ partly resolved 2026-10-20 (§9.0.4); H1–H6 open before P6-0b** | **Narrowed (§9.0.4), not investigated now [C]:** the `dgraph bulk` invocation — `--zero` target, shard counts for `--replicas=3`, output path — the alpha stop/start sequence, the Staging cluster's state at bootstrap, and topology verification. Loader choice is settled **[C]** PD-19; `p`-distribution is already tooled **[V]** | P0-8 — **due at the Local→Staging boundary**, not in P0 | **P6-0b only.** Blocks **nothing Local** (P1, P3, P4, P5.1–5.3); still **mandatory** before the Staging bootstrap, and relevant to P7-10 |
| **INV-8** | Measured ID cardinality | P0-9 | Sets the C3 threshold. Its §6.1 trigger (>1M) was **resolved by PD-22**: the in-memory design is replaced by §7.0 |
| **INV-9** | Deployed Dgraph alpha version | P0-10 | P4-10 local stack parity; P3-8 pydgraph pin |
| **INV-10** | Full JSON of the **latest ACTIVE** `GAR-Stg-Trace-Task-ETL-Dgraph` revision (§8.4 V-a, §8.6 step 1) | P0-11 | P5-8, P5-16, P5-18, P5-20 |
| ~~**INV-11**~~ | *Closed.* `stg/etl-dgraph/creds` — ARN, AWS-managed KMS key, and population with the seven `db_*` keys all settled (§8.5) | P0-12 | Nothing |
| **INV-12** | ECR tag immutability (V-d) and lifecycle protection of the Go image (V-i). Tag **availability** is **[C]** D3 | P0-13 | **Not the first push** — the second image push (P6 iterations, P6-7) and P7-3 |
| **INV-13** | Dgraph ACL posture (§8.5 A4) — **[R]** opportunistic, **not a gate** **[C]** per C2 | P0-14 | Nothing. If it turns out enabled, it is a P3-8 code change; carried as a Medium risk (§12) |
| ~~**INV-14**~~ | *Closed.* Staging `DGRAPH_ALPHAS` = `172.30.0.117:9080` **[C]** (§8.4.1) — *corrected 2026-10-16; the earlier `172.30.4.114` was a Redis instance* | P0-15 | Nothing |
| ~~**INV-16**~~ | ~~How `sync_time` returns to `NULL`~~ | — | **[C] Withdrawn (PD-20)** — external process, out of scope |

---

## 12. Risk → mitigation map

| Risk | Severity | Mitigated by |
|---|---|---|
| ~~Ordering void if `.sync` absent~~ | — | **[C] Out of scope (PD-14)** — owner-owned. Recorded in §5.2 as a consequence, not tracked here |
| Schema predicate drift at bulk reload | Medium | P7-11 |
| ~~Out-of-band invocation races Traceability~~ | — | **[C] Out of scope (PD-14)** — owner-owned |
| ~~Views go stale after the refresh leaves the ETL~~ | — | **[C] Out of scope (D2)** — MV refresh is removed from the Dgraph ETL and unrelated to Dgraph output; whatever refreshes the views is owned elsewhere and is not tracked here |
| Removal path under-validated in Staging | Medium | P1-5, scenario 3 (P1-7), local E2E (P4-12), clean-cycle criterion B4 — **not** P6-6 |
| Silent graph corruption from a porting error | Medium | P1 golden files → P4 acceptance; P5-1 harness; §10.3 Critical classification |
| Fixture realism | Medium | P1-7 seven scenarios; P6 real-data exposure; 14-day soak |
| No in-house pydgraph experience | Medium | P0-4, P0-5 spikes before commitment; local Dgraph stack (P4-10) |
| **Task fails at start on Fargate** (arch mismatch, missing endpoint, missing IAM) | Medium | P5-8, P5-16, P5-18, and the standalone `run-task` test (P5-21) **before** any workflow involvement |
| Rollback artefact overwritten by a Python push | Low | **[C]** PD-5 — separate tags; §8.6 steps 2 and 3; P7-3 lifecycle exemption |
| **Credentials already exposed by the existing Go pipeline** — `.env` baked into every image layer and published in two ADO artifacts per run | **Medium** | **[V]** `Dockerfile-stg:34`, `build.yml:61-63`, `publish-template.yml:72-74`. **Pre-existing, not introduced by this migration, and not closed by it** — the Python path avoids the mechanism entirely (PD-8), but historical images and artifacts retain the values. **[R]** Rotate the affected credentials and review ADO artifact retention on their own schedule (§15.7) |
| A tag alone cannot attribute a run to an artefact — across accounts or within one | Low | Digest recorded in the cycle log (§9.2), the deployment log (§8.6 step 8) and the task-definition reference (§8.6 step 3) |
| **Mutable `dgraph-incremental` tag makes cycle evidence unattributable** | Medium | §8.4 S3 — reference the revision by **digest** (§8.6 step 3), record digest + revision in the cycle log, reset the counter on any redeploy (§9.2) |
| **Dgraph ACL turns out to be enabled** — **[C]** C2 accepts §8.5 A4 as an **unverified assumption** | Medium | **Accepted, not closed.** **[R]** Disprove it cheaply during the P0-4 pydgraph spike (INV-13); if it holds, the cost is a P3-8 login path plus two secret keys. The P4-12 local run cannot surface it — the local alpha has no ACL either |
| **Second image push rejected by tag immutability** — D3 confirms availability, not the repository setting | Low | V-d verified before the second push. If immutability is on, the fallback is a per-build tag plus a task-definition revision per build (§8.6), which the digest-based reference already supports — no plan change, only more revisions |
| Loader and secret disagree on key names | Low | **[C]** §8.5 now records the key names **as populated**, so the loader is written to the secret rather than the reverse; P5-15 validates all seven at startup and names what is missing; the P3 test pinning the loader's required-key list to the §8.5 table keeps them from drifting apart later |
| Postgres traffic unencrypted (`sslmode=disable`) | Low | **[C]** PD-8, accepted by decision. **[V]** Identical to current Go behaviour, so the port changes nothing; reversible by editing one secret key, with no code change |
| **A revision registered before it is meant to deploy** — the family resolves forward (PD-9) | Low | §8.6 preamble: registering *is* deploying; the step 5 checklist runs before registration, not after; §9.2 resets the cycle counter on any redeploy |
| **Baseline not stamped after Initial — Day 1 processes the full dataset** | **Medium** | **[V]** Nothing in the codebase stamps the baseline (§9.0.1). Violates decision 2, and at scale breaches the §6.1 >1M guardrail and **C3** by construction. Closed by the P6-0c statement, run after the load and before seeding |
| **Baseline stamped against a load that did not actually succeed** | **Medium** | Rows would be marked synced against a graph that never received them, and **[V]** nothing ever re-nulls them to recover. Ordering rule in §9.0.1: stamp only after the load is verified |
| A seeded update is not marked `NULL` by the external process | Low | **[C]** PD-20 — out of scope. Contained rather than investigated: the harness asserts against the owner-supplied changeset (§9.0.3), so such a record surfaces as a **missing expected row**, not a silent pass |
| **`dgraph bulk` invocation undocumented** | **Medium** | Loader settled **[C]** PD-19 and `p`-distribution tooled **[V]**; the command, shard counts and stop/start sequence are not. **INV-7 — [C] deferred to the Local→Staging boundary**, mandatory before P6-0b. Accepted as deferred because no Local task touches a bulk load |
| **Cluster topology in the scripts may not be the live Staging cluster** | **Medium** | **[V]** `setup_dgraph_*.sh` describe `172.30.0.x`; the ETL targets `172.30.0.117`, and **[X]** the scripts' addresses no longer exist (the cluster was rebuilt as `alpha-2026` / `zero-2026`, 2026-10-16). Confirm before relying on those addresses for the bulk load. Carried inside **INV-7**, therefore **[C] deferred** with it |
| **Initial and Incremental run concurrently in Staging** | **Medium** | Assessment §2.1 — `initial` rebuilds the graph while `incremental` mutates it, and **no code enforces exclusion**. Closed by **P6-0d** sequencing |
| **Real data leaks into local development** | Medium | PD-3; synthetic-only fixture (P1-6); CI guard (P4-14) |
| Memory blow-up | Low | **PD-22 / §7.0** (resident memory independent of the backlog), P4-17 ceiling test, INV-8 (P0-9), criterion C1/C3, local RSS baseline (§2.1), P6-7 |
| **Nondeterministic Dgraph write if a field is both set and deleted in one mutation** | **Medium** | **[X]** Dgraph v25.3.1: same-predicate set+delete is undefined (P0-4 correction). Mitigated by P4-2's per-mutation assertion and the P1 goldens (none violate it). Residual: duplicate hierarchy rows (open data question under P4-2) |
| **Stale best-effort uid lookups** (≤ ~1 s) | Low in a daily run · **Medium in the local E2E harness** | **[X]** Measured. Run-created uids are kept for the whole run (§7.0); the harness waits after seeding (P4-12); FU-4 |
| **Run ledger lost or out of disk** | Low | Scratch by design — `sync_time` is untouched until the end, so a re-run re-derives it (P4-18); size confirmed in P5-19 |
| `BlankNode` divergence | Low | P4-3 — property only, not byte-parity |
| No test oracle | Low | P1 in full |

---

## 13. Rollback

| Stage | Rollback action | Available until |
|---|---|---|
| Local | Discard the branch; nothing deployed | Always |
| P6 (Staging) | `register-task-definition` from the saved `td-go-rollback.json` (§8.6 step 2) — the family resolves forward, so this makes the Go definition current again | Always |
| P7 soak (Production) | Same one-command procedure in Production; the self-gate no-ops harmlessly. **Triggers a soak restart** (§10.3) | **Until §10.5 begins** |
| After P7-7 | No binary rollback — the Go incremental path is removed. Recovery would require reverting the decommission commits | — |

**[R]** Rehearse rollback during P6 so it is proven rather than theoretical, and keep the Go image protected from lifecycle expiry throughout the soak (P7-3). **[C]** PD-5 keeps the Go image on its own tag, so no Go artefact is ever rebuilt or overwritten; **[C]** PD-9 makes the rollback a single `register-task-definition` from a file saved in advance. Record and revert by **digest**, not by tag (§8.4 S3).

---

## 14. Items requiring owner input

All 15 assessment decisions are carried unchanged except **#15**, which **[C]** H1 changes from *rename* to *create a new repository seeded from this one with history preserved*. PD-1 … PD-11 are carried unchanged.

### 14.1 Resolved

| # | Item | Resolution |
|---|---|---|
| **B1** | ECR repository and tag strategy | **[C]** PD-5 — existing repository, dedicated tag `dgraph-incremental`, Go image preserved and never overwritten |
| **B2** | Task-definition reference style | **[C]** PD-6 — deploy the latest applicable revision, identified and verified by §8.6; Step Functions untouched. Superseded in part by **PD-9** (C4): the reference is the family name |
| **B3** | Secret contents | **Superseded by §8.5** — resolved via C1/D1/E1: the secret is populated and §8.5 records the seven `db_*` keys as the loader contract |
| **B4** | Who registers task-definition revisions | **[C]** PD-7 — manual, per the §8.6 runbook; no pipeline or IaC automation |
| **C1** | Secret shape and TLS mode | **[C]** PD-8 — coordinates and credentials both in `stg/etl-dgraph/creds`, nothing in the task definition; `sslmode=disable`. Exact keys specified in §8.5 |
| **C2** | Dgraph ACL | **[C]** Proceed assuming ACL is **not** in use, recorded as an **[A] unverified assumption** (§8.5 A4), carried as a Medium risk (§12) and an opportunistic check (INV-13) — **not** a gate |
| **C3** | Postgres access | **[C]** Credentials are the `db_username` / `db_password` keys of the same secret (§8.5). Required privileges documented per relation in §8.5; **no user or grant is modified** |
| **C4** | Task-definition reference | **[C]** PD-9 — the **family name** `GAR-Stg-Trace-Task-ETL-Dgraph` is the intended reference. V-f withdrawn; Step Functions not inspected |
| **D1** | Secret encryption | **[C]** AWS-managed `aws/secretsmanager`. P5-16 reduces to one `GetSecretValue` statement — no `kms:Decrypt` |
| **E1a** | Secret ARN | **[C]** `arn:aws:secretsmanager:ap-southeast-3:389360470396:secret:stg/etl-dgraph/creds-0mxXQ4`. The P5-16 policy document is now written out in full in §8.5 |
| **D2** | Postgres grants | **[C]** Confirmed — the role holds the §8.5 privileges. No grant work before Staging; narrowing optional and deferred |
| **D3** | ECR tag | **[C]** `dgraph-incremental` is **unused**, so the first push is unblocked. Scoped precisely: this says nothing about tag immutability (V-d) or lifecycle policy (V-i) |
| **M1** | Memory: the ID backlog | **[C]** PD-22 (2026-10-18) — never held in memory; streamed into a disk-backed run ledger (§7.0). Replaces assessment decision 6; closes the §3 trigger |
| **M2** | Go behaviours kept by the port | **[C]** PD-23 (2026-10-18) — preserve the three behaviours; record them as follow-ups FU-1 … FU-3 (§14.3), not fixes |
| **M3** | INV-3 / P4-2 | **Corrected** 2026-10-18 on measured evidence (P0-4): same-predicate set+delete is undefined. No ETL behaviour and no golden file was changed |
| **M4** | Shared fixture | **[C]** PD-24 (2026-10-18) — owned by `ETL-Dgraph-Initial`; Python invokes it in place, pinned by `tests/fixture.lock` |
| **M5** | Real Secrets Manager validation | **[C]** PD-25 (2026-10-18) — stays at P5-21; no real secret is read before then |
| **M6** | `sync_time` atomicity | **[C]** PD-26 (2026-10-19) — keep Go's per-batch stamping; partial stamps after a failure during stamping are accepted, re-runs are safe |
| **M7** | uid LRU | **[C]** PD-27 (2026-10-19) — mandatory per P4-15; implemented and tested |
| **M8** | Ledger left behind by `SIGKILL` | **[C]** PD-28 (2026-10-19) — acceptable on ephemeral storage; no cleanup mechanism |
| **M9** | P5-21 four-phase run | **[C]** (2026-10-20) — a mandatory gate before any P6 clean cycle is declared successful (§9.2). P5 is accepted only if its acceptance criteria permit it |
| **M10** | Task role | **[C]** (2026-10-20) — keep `SecretManager`, **`AdministratorAccess` accepted explicitly**; no dedicated role and no revision for this purpose. The §8.5 policy stays a specification. The P5 least-privilege criterion is waived for Staging; its implications are recorded under P5 acceptance. Revisit for Production at P5-25 |
| **M11** | INV-7 | **[C]** (2026-10-20) — resolve **and verify** before the P6-0b bootstrap. Verification of the Staging cluster authorised. **◐** Done read-only and by local rehearsal; H1–H6 need host access (§9.0.4) |
| **M12** | Launch safety | **[C]** (2026-10-20) — nothing may launch `GAR-Stg-Trace-Task-ETL-Dgraph` before the bootstrap. **[X]** Checked read-only, **re-verified 2026-10-20 ~16:00 UTC**:<br>• 33 ECS services in 11 clusters, 29 EventBridge rules (23 with ECS targets) and 25 schedules: none targets the family;<br>• no task of the family is running;<br>• CloudTrail shows only the two P5-21 launches.<br>*Correction:* the first schedule check silently failed on Windows line endings; it was redone properly, with the same result.<br>**[C] Owner (2026-10-20): no Step Functions execution is running.** Step Functions is not inspected (PD-14). A *future* execution that launches the family before the bootstrap remains the owner's to prevent |
| **M13** | D-1 | **[C]** (2026-10-20) — **Option A: preserve Go parity.** A same-(subject, predicate) set + delete caused by disagreeing duplicate detail rows is logged as a WARN, and the mutation is sent as Go sends it. No behaviour change. Harness: EV-5 `indeterminate` for the affected fields; how that counts is the §9.2 row |

### 14.2 Outstanding items

**No owner confirmation is open.** *D-1 (P4-2) was resolved 2026-10-20: Option A, keep Go parity (M13).* As recorded before: the port detects a same-(subject, predicate) set + delete, logs a warning and sends the mutation as Go does; the P4-2 wording ("asserts") could be read as "fail the run". Confirm Go parity, or ask for the run to fail. It does not block §7.2. **It must be decided before the first counted P6 cycle:** choosing "fail" is a code change and a new revision, both of which reset the counter (§9.2). Under "Go parity", a disagreeing duplicate makes the harness verdict `indeterminate`, for which §9.2 has no rule yet. Every other item below is closed:

| # | Item | Resolution |
|---|---|---|
| **B-1** | Repository locations | **[C]** PD-21 — both are local directories alongside `Dgraph` at the paths in the header table. `ETL-Dgraph-Initial` is cloned from `Dgraph` (history preserved); `ETL-Dgraph-Incremental` is new. **Remote host, organisation and URLs are not known and not assumed** — **[C]** not required for P0-1, P1, P3, P4 or P5.1–P5.3 (§3.0) |


| # | Action | Needed before | Note |
|---|---|---|---|
| **E1** | *Done.* **[C]** Secret populated with `db_host`, `db_port`, `db_name`, `db_username`, `db_password`, `db_schema`, `db_sslmode` — recorded as the loader contract in §8.5 | — | The **values** are unverified by this plan, by design: nothing here reads them. P5-15 checks presence at startup; P5-21 is the first proof that they actually connect |

What remains is **self-service verification**, none of it blocking the first Staging deployment and none of it needing you:

| # | Verification | Needed before | Why it is not a blocker now |
|---|---|---|---|
| **E2** | ECR **tag immutability** on `gar-stg-trace-ecr-dgraph-etl` (V-d) | the **second** image push | **[C]** D3 confirms the tag is unused, so the first push succeeds regardless. Immutability would only reject a *re-push* of `dgraph-incremental` — the first bug fix or the P6-7 resize |
| **E3** | ECR **lifecycle policy** cannot expire the Go rollback image (V-i) | **P7-3** | Only matters once the soak depends on the Go image still existing |

Three **CI/CD decisions are deliberately deferred** **[C]** PD-10. None gates Staging; all are detailed in §15:

| # | Open decision | Decide before | Status |
|---|---|---|---|
| **F1** | **Pipeline hosting shape** — classic UI pipeline, YAML in a separate pipelines repository, or a minimal `extends` stub | CI/CD implementation, after Staging | **Parked** — turns on where the canonical repository lives (§15.1) |
| **F3** | **`.env` retained, replaced by ECS injection, or hybrid** for the Go image | whenever the Go image is next rebuilt | **Open by choice** — analysed in §15.6; no default assumed |
| **F4** | **Production ECR repository placement** — shared with the Go image as PD-5 does for Staging, or dedicated | P7 (P5-25) | **Open** — trade-offs in §15.3 |
| **INV-7** | **`dgraph bulk` procedure** — invocation, shard configuration, output path, alpha stop/start sequence, Staging cluster state, topology verification | **P6-0b** (Staging bootstrap) | **[C] Deferred** — taken up when Local validation completes and Staging implementation begins. **Mandatory before the Staging bootstrap; blocks nothing Local** **◐ 2026-10-20:** procedure verified locally, topology and state verified read-only; **H1–H6 open** (§9.0.4) |

Both previously open items are now **closed**:

| # | Decision | Resolution |
|---|---|---|
| **H1** | Repository strategy | **[C]** Create **two new repositories**; `ETL-Dgraph-Initial` is seeded from **this** repository with **history and commit provenance preserved** (`git push`, not a fresh `init`). The **original repository stays active** but is **no longer the source** for it — the two diverge when Staging testing begins. **Modifies assessment decision 15** from *rename* to *create-with-history*; P1-9's SHA citations remain resolvable, which was the only provenance-critical property |
| **H2** | External MV-refresh state | **[C]** Withdrawn by D2 — the plan does not investigate, verify, or manage it, and it is not a prerequisite for anything here. INV-15 deleted; P2-9 … P2-11 withdrawn; A4 removed |

Two items remain flagged as **[R] recommendations rather than confirmed decisions**, each safe to adopt or drop without reopening a decision:

| # | Recommendation | Where |
|---|---|---|
| **R1** | Clean-cycle criterion **C2** — *superseded*: the `TimeoutSeconds` threshold is gone with PD-14; C2 is now a regression band against the first clean cycle, normalised for seeded volume | §9.1 |
| **R2** | Soak safeguard — **≥10 successful cycles** within the 14 calendar days, so a low-activity window cannot satisfy PD-2 vacuously | §10.2, G3 |

Everything else outstanding is an investigation task (§11) with a named owner-phase, requiring no decision.

### 14.3 Post-refactor follow-ups — **out of scope** **[C] (PD-23)**

Recorded so they are not rediscovered. **None is fixed, mitigated or gated by this refactor**; the Python port reproduces the Go behaviour exactly (P4-19) and any change is a separate piece of work for the owner.

| # | Behaviour at baseline `b487294` | Why it may deserve a fix later | Where it is pinned |
|---|---|---|---|
| **FU-1** | An event with **no valid link** is never written to Dgraph and **never stamped**: `eventIDsQuery` requires a valid link, so it stays `sync_time IS NULL` and is re-scanned every run until a link appears | Silent: such events are absent from the graph, and the pending count never drains | scenario S7 (`l3/pipeline/S7_event_without_links.json`) |
| **FU-2** | A link whose endpoint has **no uid** is **dropped silently but still stamped** (`links.go:325-329` skips it; every fetched link id is stamped) | Data loss: the link is never written unless its row is re-nulled | P4-19 |
| **FU-3** | A **malformed uid-lookup response reads as "not found"** (`helpers.go:85-87`), so an existing event would be created again | Duplicate `unique_id` nodes | `l1/parse_lookup_json.json` |
| **FU-4** | uid lookups are **best-effort**, so they can miss a node committed up to ~1 s earlier (§6 finding) | Same effect as FU-3; a strong read-only lookup missed 0 of 300 | P4-12 harness wait |
| **FU-5** | `max_life_time_ms: 3600` is **3.6 s**, probably meant as one hour; Go's mutation backoff **overflows** past attempt ~36 | Harmless today; a latent unit/overflow defect | `ETL-Dgraph-Incremental/README.md` observations 1–2 |

**Observed in P4, reproduced exactly, not yet classified by the owner** (candidates for this table; nothing about them is changed):

| # | Behaviour at baseline `b487294` | Possible consequence | Where it is pinned |
|---|---|---|---|
| **O-1** | Several valid `links_bc` rows for one `(event1, event2)` are **one** Dgraph edge; each write replaces its facets, and `linkDetailsQuery` has no `ORDER BY` while link upserts run concurrently, so **which row's facets survive is unspecified** | Non-deterministic facet values, if such rows exist (unverified) | `test_parity_guards.py` |
| **O-2** | Removal's `<uid> * * .` deletes the node's own predicates and outgoing edges; **other events' `links` to it remain** (Go golden `remove_invalid_events`: `KEEP → X1` after `X1` is gone) | Dangling edges to an empty node | L2 golden `remove_invalid_events`; to be confirmed against a real alpha in P4-12 |

---

## 15. CI/CD direction — deferred **[C] (PD-10)**

Recorded so the direction is not re-derived later. **Nothing in this section is a prerequisite for Staging, and no item here blocks any task in §1–§14.**

### 15.1 Scope, timing and hosting

**[C]** This repository is a **mirror**; no CI/CD pipeline is directly affected by it. Design and implementation are both deferred until Staging completes.

**Why Staging is unblocked:** the Staging path needs an image in ECR and a registered task-definition revision. The first is a local `docker build` and `docker push`; the second is manual by decision (PD-6, PD-7, §8.6). P5-21 proves the image runs on Fargate without any pipeline involvement.

**[?] Hosting shape — F1, parked.** One durable constraint to carry forward: **[X]** Azure DevOps supports CI triggers on a **repository resource** for **Azure Repos Git only, not GitHub**. So if the canonical repository is GitHub-hosted, a YAML pipeline whose definition lives in a *different* repository cannot itself trigger on a push here, and the realistic options narrow to:

| Option | Files in this repo | Notes |
|---|---|---|
| **A** — classic (UI-defined) pipeline | none | Meets "no pipeline definitions in the repository" literally; pipeline logic is not version-controlled or reviewable |
| **B** — YAML in a separate Azure Repos pipelines repository | none | Clean and reviewable; requires the code to live in, or be mirrored to, Azure Repos |
| **C** — minimal `extends` stub pointing at an external template | one small file | All logic decoupled; strictly speaking a file remains |

**[V]** The working clone's remote is `github.com/fajartirtayasa/Dgraph.git`, and **[C]** it is a mirror — so the canonical host is **[?]** and the choice cannot be made from repository evidence.

### 15.2 Target branch and environment model **[C] (PD-11)**

| Branch | Environment | AWS account |
|---|---|---|
| `stg` | Staging | Staging account |
| `prd` | Production | **Separate** account **[C]** |

**Dev is removed** — not an actual environment **[C]**. Dropping out of the target model: the `gar-dev` branch, `deployment/dockerfiles/Dockerfile-dev`, `deployment/pipelines/stages/dev.yml`, the `trace-dgraph-etl-dev` variable group, and the ADO `dev` Environment **[V]**.

This **supersedes `REFACTOR_ASSESSMENT.md` §2.4 R4**, which recorded the branch triggers (`gar-dev`, `gar-stg`, `gar-prd-1`) as unaffected by the repository move. They are affected — by this change, not by the move.

### 15.3 ECR repository placement

**Current state [C]:** the Go ETL uses `gar-stg-trace-ecr-dgraph-etl` with the **`latest`** tag.

**[C]** PD-5 places the Staging Python image in that same repository under `dgraph-incremental`, and that stands. **[?] F4 — whether Production repeats the sharing is open.** Trade-offs, for the later decision only:

| Shared repository | Dedicated repository per ETL |
|---|---|
| Go rollback image and Python image coexist; separation rests on **tag discipline alone** | Separation is structural |
| One lifecycle policy governs both — a rule written for one can expire the other's rollback artefact (P7-3) | Independent lifecycle and immutability per ETL |
| No new AWS resource | Requires creating a repository |

**[R]** Dedicated repositories are the cleaner end state, mainly because the Go image must survive untouched through the 14-day soak while the Python image is pushed repeatedly. A recommendation for the F4 decision, **not** a change to PD-5.

### 15.4 Tags, digests and two accounts **[C] (PD-11)**

**[C]** Staging and Production use the **same tag naming convention** in **separate AWS accounts**. That is safe without further qualification: **[X]** an ECR repository is scoped to one account and one region, and the full reference is `<account-id>.dkr.ecr.<region>.amazonaws.com/<repository>:<tag>`. The same tag in two accounts names two unrelated objects — no collision, nothing shared, no cross-account permission implied. **[C]** Repository *names* need not match either.

| | Tag | Digest |
|---|---|---|
| What it is | a **mutable label** within one repository | **`sha256:…`**, the immutable content hash |
| Scope | per repository, per account | global — identical content, identical digest |
| Answers "what ran?" | **no** | **yes** |

Two consequences, both already reflected in the plan:

1. **A tag cannot identify what ran** — hence the digest in the cycle log (§9.2), the deployment log (§8.6 step 8), and the task-definition reference (§8.6 step 3).
2. **[A] Same tag and same commit does not mean same bytes.** If each branch builds independently, Staging and Production images carry **different digests even from identical source**, because container builds are not reproducible by default. The shared convention is a naming rule, not an equivalence claim.

**[R] Artifact promotion — available, not adopted.** If "Production runs exactly what Staging validated" ever becomes a requirement, the mechanisms are **digest promotion** (pull the Staging digest, retag, push to the Production account) or **ECR cross-account replication**; both preserve digest identity. Both replace the build-per-branch model, so neither is assumed here. Worth weighing against P7 — the 14-day soak gains strength if the soaked bytes are provably the validated ones.

### 15.5 Repository cleanup and the build contract

Applies whenever CI/CD is implemented; **not performed now**.

**Removable** **[V]**: `deployment/pipelines/**` (6 files) · `Dockerfile-dev` and `Dockerfile-prod` (the latter referenced by **no stage**) · and, if the environment Dockerfiles collapse onto the root one, `Dockerfile-stg` and `Dockerfile-prd-1` — **all four are byte-identical**, differing from the root `Dockerfile` only by `COPY .env` and cosmetics (`EXPOSE 8080`; the binary is named `traceability-dgrapg-etl`, a transposition).

**Must stay:** root `Dockerfile` · `go.mod` / `go.sum` / `main.go` / `internal/` / `pkg/` · `config/app.yaml` · `schema.dql` · `.env.example`.

**Worth adding** — both documentation rather than automation:
- **`.dockerignore`**, currently **absent** **[V]**. The working tree already carries 31 MB of untracked build output (`out/` 16 MB, the `traceability-dgraph-etl` binary 15 MB) **[V]** plus `.git`, all of which a build context would otherwise absorb.
- **`deployment/README.md`** — the **build contract** an external pipeline must satisfy, which is what replaces the YAML: Dockerfile path, `linux/amd64` platform **[V]**, tag convention, what the image does *not* contain, and the explicit statement that **publishing an image is not deploying it**.

**Constraints any future pipeline must honour** (carried from the withdrawn P5-24): push only to the ETL's own tag, never the Go image's tag (PD-5); **do not** generate or bake credentials; **do not** register task definitions (PD-7, §8.6); surface the pushed **digest** as an output. **[R]** Add a `go vet` / `go test` gate once P1 exists — **[V]** the current pipeline gates only on compilation, as a side effect of `docker build`.

### 15.6 `.env` handling — **[?] open (F3)**

**No default is assumed in either direction.** The relevant verified facts:

| Fact | Evidence |
|---|---|
| `.env` is **optional at runtime** — a missing file logs `"No .env file found"` and startup continues | **[V]** `main.go:34-36` |
| Configuration resolves `config/app.yaml` → overridden by environment variables | **[V]** `internal/app/config.go` |
| `godotenv.Load()` does **not** overwrite variables already set | **[X]** library semantics |
| The deployment Dockerfiles **hard-fail** without `.env` — `COPY .env` errors if absent | **[V]** `Dockerfile-stg:34` |

**So `.env` is not technically necessary** — every value it carries can arrive as an environment variable, which takes precedence over it. The question is mechanism, not capability.

| | **A — `.env` via File Creator** (status quo) | **B — ECS task-definition injection** | **C — runtime SDK fetch** |
|---|---|---|---|
| Go code change | none | **none** — env vars already override | **required** — **[V]** no Secrets Manager client exists (`pkg/aws` is S3-only, unused) |
| Credentials in the image | **yes**, readable by anyone who can pull | no | no |
| Credentials in ADO artifacts | **yes**, twice per run **[V]** | no | no |
| Rotation | rebuild + redeploy | new revision, or none if the secret is updated in place | next run |
| Image environment-specific? | **yes** — forecloses digest promotion (§15.4) | no | no |
| IAM | — | **task *execution* role** needs `GetSecretValue` **[X]** | **task role** — the Python pattern (P5-16) |

Note the asymmetry: ECS injection authorises the **execution** role; the Python ETL's runtime fetch authorises the **task** role. Same secret, different role.

**[R] Hybrid, if File Creator is wanted for ergonomics:** generate `.env` with **non-secret tunables only** and inject credentials via ECS `secrets`. **[X]** `valueFrom` supports JSON-key selection (`<secret-arn>:<json-key>::`), so the already-populated `db_*` keys in `stg/etl-dgraph/creds` (§8.5) map one-to-one onto `DB_CLEAN_*` variables with no code change.

**Bearing on the Python ETL: none.** **[C]** PD-8 and P5-15 already settle it — runtime `boto3` fetch, no `.env` in the image. F3 concerns the **Go** image only.

**What would settle F3:** whether the Go ETL (post-decommission, `initial`-only) is invoked via ECS at all or run ad hoc **[?]**; whether Staging and Production should be able to run identical bytes (§15.4); and whether the execution-role/task-role split is acceptable alongside the Python pattern.

### 15.7 Credential exposure — pre-existing, not closed by this plan

**[V]** Every past run of the Go pipeline wrote credentials into `.env` and then into (a) an image layer and (b) **two** published ADO artifacts (`DockerBuildContext`, `image-pushed-context-<tag>`). Artifact read access is typically broader than ECR pull access, and retention outlives the image.

**[V]** No `.env` is tracked in git (`.gitignore:27`; `git ls-files` shows none), so this is a CI and registry exposure, not a source-control one.

**[R]** Independent of F3 and of the migration: rotate the credentials that passed through that mechanism, and review ADO artifact retention. Recorded here so it is tracked; it is **not** a Staging task and **not** a migration deliverable.

