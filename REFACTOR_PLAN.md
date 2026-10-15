# REFACTOR_PLAN.md

Phased execution plan: migrating the incremental ETL from Go to Python.

**Source of truth:** `REFACTOR_ASSESSMENT.md`. This plan translates its 15 confirmed decisions, architecture, and risk register into executable phases. It does not introduce new architectural decisions; anything unresolved in the assessment appears here as an **explicit investigation task**, never as an assumption.

**Markers (carried from the assessment):** **[C]** confirmed decision · **[V]** verified in the Go repository · **[X]** verified externally · **[A]** assumption · **[R]** recommendation · **[?]** requires investigation · **[O]** owner-owned (outside this plan's execution scope; the plan consumes the result) · **⏳** pending owner input.

**Repositories** **[C]**
- **`ETL-Dgraph-Initial`** — Go, renamed from the existing repository, history preserved (baseline `ad372de`), feature-frozen
- **`ETL-Dgraph-Incremental`** — Python, new; owns canonical `schema.dql`

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
| **PD-11** | **Target branch model — `stg` and `prd` only.** **Dev is removed**; it is not an actual environment. The two environments live in **separate AWS accounts** and share **one tag naming convention**, which is safe because an ECR repository is scoped to one account and region | **[C]** §15.2, §15.4 |

---

## 1. Phase overview

| Phase | Name | Env | Repo | Gates | **[A]** Effort |
|---|---|---|---|---|---|
| **P0** | Investigations & Baseline | — | both | everything | 1–2 wks |
| **P1** | Parity Harness | Local | Initial (Go) | **all Python work** | 2–4 wks |
| **P2** | Orchestration Validation (Go binary) **[O]** | Staging | Initial + AWS | P6 | 1–2 wks *(parallel)* |
| **P3** | Python Foundation | Local | Incremental | P4 | 1–2 wks |
| **P4** | Port Incremental Logic + Local E2E | Local | Incremental | P5 | 3–6 wks |
| **P5** | Verification Harness, Container & Deployment Infra | Local → Staging | Incremental + AWS | P6 | 2–3 wks |
| **P6** | Staging Validation | Staging | Incremental | P7 | ≥5 clean cycles **[C]** |
| **P7** | Production Cutover & Decommission | Production | both | — | ≥14-day soak **[C]** |

### Dependency graph

```
P0 ─┬─> P1 ─> P3 ─> P4 ─> P5 ─> P6 ─> P7
    │                            ↑
    └─> P2 [O] ──────────────────┘   (parallel; off critical path)

Local ──────────────────────────> Staging ──────> Production
 P1 · P3 · P4 · P5.1–5.3           P2 · P5.4 · P6      P7
```

**Critical path:** P0 → P1 → P3 → P4 → P5 → P6 → P7. **[A]** ~3–4 months to cutover, plus the 14-day soak before decommission. Validation, not coding, is the long pole.

**CI/CD sits outside this phase model.** **[C]** PD-10 — deferred until Staging completes and recorded in §15; no phase depends on it.

**[R] Start P2 early.** Per assessment §5.6, validating orchestration with the *Go* binary de-risks Step Functions independently of the port. Keeping it off the critical path costs nothing and removes ambiguity from the first Staging failure. **[O]** Owner-executed (PD-4).

---

## 2. Environment progression **[C] (PD-3)**

Three environments, entered strictly in order. Each has its own objective, prerequisites, task set, and exit criteria. Nothing in a later environment starts before the previous one's exit criteria are recorded as met.

### 2.1 Local

**Objective.** Prove logic equivalence and end-to-end correctness against a fully controlled stack — Postgres *and* Dgraph both running locally — before any AWS resource is touched.

**Data rule [C].** **Synthetic/dummy data only.** No dump, extract, sample, or anonymised copy of Staging or Production data is used locally, at any point, for any reason. The fixture built in P1-6/P1-7 (seven scenarios, synthetic) is the *only* local data source and is shared verbatim by the Go and Python sides.

**Prerequisites.**
- P0-0 (baseline commit), P0-1 (repository decision — **H1**) and **P0-6** (schema-application procedure, INV-5) complete
- Local Dgraph version pinned to the deployed cluster version (INV-9)

**Tasks.** P1 in full · P3 in full · P4 in full, including the local end-to-end block (§7.2) · P5.1–P5.3 (harness, image, local container rehearsal).

**Exit criteria.**
- ☐ P1, P3, P4 acceptance criteria all met
- ☐ All **7 scenarios** pass end-to-end against local Postgres + local Dgraph, from the Python ETL
- ☐ **Every P1 golden file reproduced byte-for-byte** by the Python port
- ☐ The **built container image** runs the full pipeline against the local stack and **exits 0**; a forced failure exits **non-zero**
- ☐ The verification harness passes against a local run **and** fails its negative test (injected discrepancy)
- ☐ No real-data artefact exists in the repository or the local stack — verified by the **test-suite guards** in P4-14 **[C]** (D4), which run offline and need no credentials
- ☐ Peak RSS measured locally and recorded as the first comparison point against the §6 projection

### 2.2 Staging

**Objective.** Validate correctness, Go↔Python parity, performance, memory, and operational behaviour at real scale. **[C]** Staging is representative of Production (assessment decision 8), so this is where scale validation happens — not Production.

**Prerequisites.**
- Local exit criteria met
- Staging deployment configuration verified (§8.4) and a new task-definition revision registered
- **[O]** P2 acceptance: the orchestration change validated with the Go binary, halt-on-failure and `.sync` confirmed

**Not a prerequisite: CI/CD.** **[C]** PD-10. Staging needs an image in ECR and a registered revision — a local `docker build` and `docker push` produce the first, and §8.6 already registers the second by hand. Pipeline work is §15, deferred.

**Tasks.** P5.4 (deploy infra against the provided Staging resources) · P6 in full.

**Exit criteria.**
- ☐ **≥5 consecutive clean daily cycles** per PD-1 (§9.1 dimensions A1–A4, B1–B4, C1–C5)
- ☐ Go↔Python parity: the verification harness reports **zero** discrepancies across the whole count
- ☐ Performance: wall-clock ≤70% of `TimeoutSeconds` **[R]**, per-phase durations recorded
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
| **Which** Step Functions workflow triggers the task, and **where** in it | **[O] Owner** |
| State definition: `.sync` integration, `Catch`/`Next`, retry, timeout, concurrency guard, alarms | **[O] Owner** |
| Matview-refresh state placement (P2-9 … P2-11) | **[O] Owner** |

The plan treats owner-owned outcomes as **inputs**: INV-1 and INV-2 are recorded as answers received, not as work performed here. Where a phase depends on one (P6 depends on P2), the dependency stands — only the execution sits elsewhere.

---

## 3. P0 — Investigations & Baseline

**Objective.** Convert every open `[?]` in the assessment into a verified fact, and establish the repository baseline. No implementation.

**Dependencies.** None.

### Tasks

| ID | Repo / Area | Task | Source |
|---|---|---|---|
| ☐ P0-0 | Repo **[O] owner** | **Baseline commit — owner will perform.** Commit the current working tree so P1 golden files cite a resolvable SHA. **Must include the MV-refresh removal** (`materialized_views.go` deletion + the `migrator.go` call), because P1 encodes the behaviour that will be ported. **[R]** Split into two commits — the behavioural removal alone, then documentation — so the removal stays independently reviewable and revertible. **Record the resulting SHA: it supersedes `ad372de` as the P1 baseline.** **Sequencing guard:** committing the removal does not change any deployed behaviour, but **no Go image may be rebuilt from this baseline and deployed until the downstream refresh state is confirmed live** (P2-11, INV-15) |
| ☐ P0-1 | Initial | Rename repository to `ETL-Dgraph-Initial`, preserving history and `ad372de`. Update git remotes and the Azure Pipelines repository reference. **[V]** No import rewrites needed — `go.mod` declares a bare module name | §2.4 |
| ☐ P0-2 | AWS **[O]** | **INV-1 (highest priority):** Confirm the ECS task is invoked with `.sync` (`arn:aws:states:::ecs:runTask.sync`). **Without it the ordering guarantee replacing the gate does not exist.** Owner-provided answer | V1 |
| ☐ P0-3 | AWS **[O]** | **INV-2:** Confirm halt-on-failure is enforced in the Step Functions state definition (`Catch`/`Next`); document existing retry policy, timeout, concurrent-execution guard, failure alarms. Owner-provided answer | V2 |
| ☐ P0-4 | Python | **INV-3:** Spike — can pydgraph set `set_nquads` **and** `del_nquads` on one mutation? **[X]** Both kwargs exist; simultaneous use unconfirmed. Required by the set-or-delete semantics | V3 |
| ☐ P0-5 | Python | **INV-4:** Spike — can gRPC gzip be enabled via raw channel options under pydgraph? **[X]** Absent from its API; **[V]** always on in Go | V4 |
| ☐ P0-6 | Ops | **INV-5:** Document the schema-application procedure (manual today) and verify whether the deployed schema matches `schema.dql` | V5 |
| ☐ P0-7 | AWS | **INV-6:** Confirm Fargate networking — is an interface VPC endpoint for `com.amazonaws.ap-southeast-3.secretsmanager` required (private subnets without NAT)? Extend to ECR, ECR-DKR, S3 (layer pulls) and CloudWatch Logs endpoints | V6 |
| ☐ P0-8 | Ops | **INV-7:** Locate and document the bulk-loader invocation (`dgraph bulk --schema`); it becomes a cross-repo runbook step | V7 |
| ☐ P0-9 | Data | **INV-8:** Run `SELECT COUNT(*)` over `eventIDsQuery` and `linkEvent1IDsQuery` against Production. Converts the **[A]** 2–3× amplification into a verified figure | §6.1 |
| ☐ P0-10 | Ops | **INV-9:** Record the **deployed Dgraph alpha version** so the local container (§7.2) and the pydgraph client version are pinned to match it | new |
| ☐ P0-11 | AWS | **INV-10:** Dump the full JSON of the **latest ACTIVE** `GAR-Stg-Trace-Task-ETL-Dgraph` revision (§8.4 V-a, via §8.6 step 1) | new |
| ☑ P0-12 | AWS | **INV-11 — closed.** `stg/etl-dgraph/creds` is populated (seven `db_*` keys), ARN provided, encryption **[C]** D1 AWS-managed, contract recorded in §8.5. Values are first exercised at P5-21 | new |
| ☐ P0-13 | AWS | **INV-12:** **[C]** D3 confirms the `dgraph-incremental` tag is **unused and available for the first push**. Still to verify, and only before the **second** push: whether repository **tag immutability** is enabled (V-d), and whether the **lifecycle policy** can expire the Go rollback image (V-i) | new |
| ☐ P0-15 | Ops | **INV-14:** Obtain the Staging **`DGRAPH_ALPHAS`** endpoint list by one of the §8.4.1 methods and record it in the deployment log. **[V]** There is no usable default — an empty list fails at client construction | new |
| ☐ P0-16 | AWS **[O]** | **INV-15:** **Does a separate downstream Step Functions matview-refresh state already exist?** **[C]** D2 — the original repository **retains** MV refresh inside its ETL, which is evidence that a separate state may **not** exist, and conflicts with the note in `CLAUDE.md`. Resolving this decides whether P2-9 **adds** a state or merely **verifies** one | new |
| ☐ P0-14 | Ops | **INV-13 [R] opportunistic:** Is **Dgraph ACL enabled** on the Staging cluster? **[C]** C2 — proceed assuming not; this is an **unverified assumption** (§8.5 A4), not a gate. Cheapest moment to disprove it is during the P0-4 pydgraph spike, against a non-production alpha | new |

### Deliverables
- Renamed repository with intact history
- Investigation report answering INV-1 … INV-15 (INV-11 closed; INV-13 opportunistic)
- Verified ID cardinality figures
- Verified Staging deployment inventory (§8.4)

### Acceptance criteria
- ☐ `git log` in `ETL-Dgraph-Initial` shows `ad372de` as the baseline commit
- ☐ INV-1 answered **yes**, or a remediation task is raised before P2 begins **[O]**
- ☐ INV-3 answered yes, or a documented fallback (two sequential mutations) is agreed
- ☐ Measured ID cardinality recorded and compared against the §6 projection of ~200–300k
- ☐ INV-9 … INV-13 answered (INV-13 **[R]** opportunistic, not a gate), and §8.4's **V-a, V-b, V-c, V-e, V-g** resolved before P5.4 begins. **V-d** is due before the *second* image push and **V-i** before P7-3 — neither gates the first deployment
- ☐ `stg/etl-dgraph/creds` populated by the owner per the §8.5 specification, with its full ARN recorded

> **[R] Stop-and-reassess trigger:** if INV-8 returns **>1M IDs**, the in-memory design (assessment decision 6) must be revisited in favour of keyset pagination per §6.1 before P4 begins. Data-driven, not a reopened decision.

---

## 4. P1 — Parity Harness (Go) · *Local*

**Objective.** Build the test oracle that does not exist today, **in Go, against the current implementation**. Per assessment §3, tests written after the port would certify the port against itself.

**Dependencies.** P0-1.

**Repo.** `ETL-Dgraph-Initial`.

### Tasks

| ID | Layer | Task |
|---|---|---|
| ☐ P1-1 | L1 | Unit-test pure functions: RDF literals, `buildLinkFacets`, `chunkIndices`, normalizers. **[V]** No infrastructure required |
| ☐ P1-2 | L2 | Implement a fake `dgraph.Adapter` capturing RDF payloads. **[V]** Returns plain types (`*DQLResponse`, `map[string]string`, `error`) |
| ☐ P1-3 | L2 | Golden-file tests over `upsertEvents`, `upsertLinks`, `deleteExistingLinks`, `cleanupMissingLinks`. **[V]** All take plain structs — no DB needed |
| ☐ P1-4 | L2 | Explicitly cover the four silent-corruption invariants: set-or-delete per field; chunk-by-`event1_id` link replacement; facets only for non-empty values; existing-uid vs new-blank-node subject selection |
| ☐ P1-5 | L2 | **Delete-payload coverage for the removal path.** Assessment §4.2: this is what Staging cannot validate without a restore |
| ☐ P1-6 | L3 | Stand up a reproducible Postgres fixture, seeded with **synthetic data only** **[C]**. **This fixture is also the local stack's database (§8.2)** — build it once, use it in both languages and both layers |
| ☐ P1-7 | L3 | Seed all **seven scenarios** **[C]**: new event · updated event · **invalidated event** · link added · link invalidated · event2-only change · event with no links |
| ☐ P1-8 | L3 | Expected-rowset tests for `eventIDsQuery`, `linkEvent1IDsQuery`, `removalQuery`, `eventDetailsQuery`, `linkDetailsQuery`. **[R]** These port to Python unchanged — the SQL strings are identical |
| ☐ P1-9 | — | Export golden files in a language-neutral format; stamp each with the generating commit SHA |

### Deliverables
- `go test ./...` suite covering L1–L3
- Committed golden-file corpus with SHA provenance
- Fixture database reproducible from a script, from synthetic generators only

### Acceptance criteria
- ☐ `go test ./...` green on `ETL-Dgraph-Initial`
- ☐ All 7 scenarios represented in the fixture, each with at least one golden file
- ☐ Scenario 3 (invalidated event) produces captured **delete** RDF, not only set/del triples
- ☐ Every golden file cites a resolvable commit SHA **[C]** (history preserved by the rename)
- ☐ Fixture DB rebuilds from scratch via a single documented command
- ☐ The fixture is generated, not extracted: **no row traceable to Staging or Production** **[C] (PD-3)**

> **This phase gates all Python work.** No P3 task starts until acceptance is met.

---

## 5. P2 — Orchestration Validation with the Go Binary **[O]** · *Staging*

**Objective.** Validate the Step Functions change *independently of the language port*, per assessment §5.6. The Go binary's self-gate becomes a harmless no-op there (it checks, finds `Done`, proceeds).

**Owner-owned (PD-4).** Retained here because P6 depends on its acceptance. This plan does not investigate workflow placement or author the state definitions; it supplies the ETL-side contract and consumes the result.

**Dependencies.** P0-2, P0-3, **INV-15** (P0-16).

**[R] Run in parallel with P1/P3/P4.** Off the critical path.

> **[C] D2 — MV refresh diverges between repositories.** The **original** repository **retains** the matview refresh inside its ETL; **this mirror removes it** (P0-0). Two consequences, and they are different in kind:
>
> 1. **Removing the code is a repository change with no runtime effect.** Nothing deployed changes until an image built from the new baseline is deployed. That is what P2-11's sequencing guard governs.
> 2. **Whether a separate refresh state exists is an AWS fact, not a code fact** — and it is now **[?] open (INV-15)**. The original retaining the refresh is evidence that no separate state exists, which conflicts with the note in `CLAUDE.md`. Until INV-15 is answered, it is unknown whether P2-9 **adds** a state or merely **verifies** one, and whether clean-cycle criterion **A4** is measurable at all.
>
> **P2's scope and dependencies are unchanged pending the owner's review of §5.1.** Nothing below has been re-scoped.

### 5.1 Why P2 exists — and what depends on it

**The Python ETL deletes its own gate.** **[C]** Assessment decisions 9 and 11: `traceability_check.go` is not ported, and ordering becomes the orchestrator's responsibility. P2 is the phase that proves the replacement actually holds **before** the replacement is load-bearing.

**What it delivers, and why each matters:**

| Deliverable | Without it |
|---|---|
| Empirical `.sync` confirmation (P2-2) | **[V]** Step Functions fires and forgets; the next state starts immediately and the ordering guarantee that replaced the gate **does not exist** |
| Halt-on-failure evidence (P2-3) | A failed Traceability day lets the chain continue, and the ETL processes incomplete data — assessment §5.3 **B2**: the failure mode inverts from *silent stall* to **silent bad data** |
| Retry, timeout, concurrency guard, alarms (P2-4 … P2-7) | Retries may overlap and race on `sync_time`; failures are invisible |
| Matview-refresh state (P2-9 … P2-11) | The three views go stale from the first Python run, silently |

**Why P6 depends on it — two distinct reasons:**

1. **Measurement.** Clean-cycle criteria **A1** (no workflow timeout), **A3** (no retry) and **A4** (refresh completed) are observed *through the workflow*. Outside it, they cannot be evaluated, so PD-1's count cannot be kept.
2. **Attribution.** This is the reason the assessment (§5.6) put P2 first. Validating orchestration with the **Go** binary — whose self-gate is a harmless no-op there — means that when something fails during P6, it is already known whether the orchestration is sound. Without that, the first Staging failure has two candidate causes and no way to separate them.

**Is P2 essential to Staging acceptance?** The parts are not equal:

| Part | Standing |
|---|---|
| **Ordering (`.sync`) and halt-on-failure** | **Essential, and before the Python ETL touches real Staging data.** Not a validation nicety — without it the ETL can silently process an incomplete day |
| **Matview-refresh state** | **Essential for A4**, and essential for correctness downstream once the Python ETL stops refreshing |
| **Retry, timeout, alarms, concurrency guard** | Strongly desirable; a gap here degrades evidence quality rather than correctness |
| **Validating with the *Go* binary specifically** | **[R]** A sequencing choice, not a requirement. Its value is attribution. Skipping it does not make Staging invalid — it makes the first failure ambiguous |

**Could it be managed separately?** Partly. **[V]** P5-21 already proves the container end to end via standalone `run-task`, with no workflow involved, so container-level defects surface without P2. What cannot be obtained separately is A1/A3/A4 evidence and the ordering guarantee. **[R]** If P2 lags, one option is to run standalone daily cycles to accumulate **provisional** cycles that exercise the ETL and the harness but do **not** count toward PD-1 — keeping validation moving without weakening the exit threshold. Recorded as an option; **not adopted, and P2's dependencies are unchanged.**

### Tasks

| ID | Task |
|---|---|
| ☐ P2-1 | Insert the existing Go ECS task into the workflow immediately after the Traceability task **[C]** |
| ☐ P2-2 | Verify `.sync` behaviour empirically — the next state must not start until the task reports completion |
| ☐ P2-3 | Verify halt-on-failure **[C]**: force a Traceability failure and confirm the Dgraph ETL state is **not** entered |
| ☐ P2-4 | Configure `Retry` with exponential backoff. **[V]** No application work needed — the run is already idempotent (assessment §5.4) |
| ☐ P2-5 | Set `TimeoutSeconds` from observed durations, not guesses |
| ☐ P2-6 | Confirm the concurrent-execution guard: a retry must not overlap an in-flight run |
| ☐ P2-7 | Configure failure alarms/notifications on the new state |
| ☐ P2-8 | Define the operational rule for out-of-band invocation (assessment §5.3 B1); **[R]** prefer an IAM/ECS guard over convention |
| ☐ P2-9 | **Add — or, if INV-15 shows one already exists, verify — a dedicated matview-refresh state** **[C]** immediately *after* the Dgraph ETL state: `REFRESH MATERIALIZED VIEW CONCURRENTLY mv_events`, then non-concurrent `monitoring_cache_shipments` and `mv_shipment_list`, sequentially, aborting on first failure |
| ☐ P2-10 | Give that state its own retry, timeout and alarm configuration; a refresh failure must be visible and must **not** be attributed to the ETL. **[R]** The refresh is idempotent, so retry is safe |
| ☐ P2-11 | **Sequencing guard:** the refresh state must be live in an environment **before** an ETL image without `materialized_views.go` is deployed there, or the views silently go stale |

### Contract supplied by this plan to P2

So the owner can configure the state without reading the ETL source:

| Property | Value |
|---|---|
| Invocation shape | One-shot batch container; no ports, no server, no readiness probe |
| Success signal | Process **exit code 0** |
| Failure signal | Any **non-zero** exit code; the task reports it as a stopped container with a non-zero exit |
| Retry safety | **Safe at any point.** **[V]** `sync_time` is stamped only in the final step, so an interrupted run reprocesses its whole changeset |
| Expected duration | Supplied from P6 measurements; `TimeoutSeconds` is sized from them, not guessed |
| Concurrency | **Must not overlap itself** — two concurrent runs race on `sync_time` and link replacement |

### Deliverables
- Go ETL running under Step Functions in Staging
- Matview-refresh state live downstream of the ETL state
- Documented retry, timeout, and alarm configuration
- Evidence of halt-on-failure behaviour

### Acceptance criteria
- ☐ A forced Traceability failure demonstrably prevents the ETL state from executing
- ☐ `.sync` confirmed: measured task duration matches state duration
- ☐ A deliberately failed ETL run retries and succeeds without data anomalies — **[V]** expected, since nothing is stamped before the final step
- ☐ Timeout and alarms fire as configured in a test
- ☐ The three views are observably refreshed by the new state after an ETL run, and a forced ETL failure leaves the refresh state unentered

---

## 6. P3 — Python Foundation · *Local*

**Objective.** Stand up `ETL-Dgraph-Incremental` with infrastructure code only — no business logic.

**Dependencies.** P1 (acceptance), P0-4, P0-5, P0-7.

### Tasks

| ID | Task |
|---|---|
| ☐ P3-1 | Create the repository; establish layout consistent with sibling Python ETLs **[X]** |
| ☐ P3-2 | **Move canonical `schema.dql` here** **[C]** (assessment §2.3) |
| ☐ P3-3 | Config loader: YAML + env for non-secret tunables (batch sizes, concurrency, chunk size) |
| ☐ P3-4 | Secrets Manager integration, region `ap-southeast-3` **[C]**; fetch **once at startup**, not per connection. **Never** bundle `.env` |
| ☐ P3-5 | **Credential-source switch** for local development: `SECRETS_SOURCE=aws` (default, used in every deployed environment) or `secrets_source=env` (local only, reading a **gitignored, never-COPY'd** `.env`). **[R]** This preserves assessment decision 10 — the image still contains no credentials and the deployed default is unconditionally Secrets Manager |
| ☐ P3-6 | `psycopg2` `ThreadedConnectionPool` **[C]**. **[R]** Connections are not safe for concurrent use across threads |
| ☐ P3-7 | Server-side (named) cursors for the large ID queries |
| ☐ P3-8 | pydgraph **synchronous** client wrapper **[C]**, version pinned against INV-9; apply INV-3/INV-4 findings |
| ☐ P3-9 | Re-implement the retry classifier — **[V]** `isRetryableError`, gRPC codes `Aborted`/`Unavailable`/`DeadlineExceeded`/`ResourceExhausted` plus ~18 string patterns |
| ☐ P3-10 | Thread fan-out primitive replacing `fanOutBatches`. **[V]** Config defaults are `concurrency: 4`, batches 50–100 — a small `ThreadPoolExecutor` suffices. **[R]** One pydgraph `Txn` per thread |
| ☐ P3-11 | Structured JSON logging to stdout (CloudWatch-compatible); `PYTHONUNBUFFERED=1` so no line is lost on exit |
| ☐ P3-12 | Port L1 tests from P1-1 |
| ☐ P3-13 | **Do not port** `StringInterner` **[C]** (§6.1) or `traceability_check.go` **[C]** (assessment decision 9) |

### Deliverables
- Repository scaffold with config, secrets, DB pool, Dgraph client, concurrency primitive
- L1 test suite passing in Python

### Acceptance criteria
- ☐ L1 parity: Python reproduces every P1-1 expected value
- ☐ Secrets resolve from `ap-southeast-3` at runtime; **no `.env` in the image or repository**
- ☐ `SECRETS_SOURCE` defaults to `aws`; an image built for deployment cannot be made to read a bundled file
- ☐ Connection pool survives a concurrency soak at 4× configured workers
- ☐ Retry classifier reproduces Go's verdict across the full gRPC-code and string-pattern matrix
- ☐ No tzdata dependency **[V]** — removed with the gate

---

## 7. P4 — Port Incremental Logic + Local End-to-End · *Local*

**Objective.** Port the business logic, validating each step against P1 golden files, then prove the whole pipeline end-to-end against a local Postgres **and** a local Dgraph.

**Dependencies.** P3 (acceptance).

**[R] Order is prescribed:** **`links` is last** — it carries the chunk-by-`event1_id` rule, the subtlest invariant in the codebase.

### 7.1 Port tasks

| ID | Step | Task | Invariant to preserve |
|---|---|---|---|
| ☐ P4-1 | removal | Port `removalQuery`, uid lookup, node deletion | Delete-by-uid; re-run no-ops |
| ☐ P4-2 | events | Port `eventIDsQuery`, chunking, `eventDetailsQuery`, upsert | **Set-or-delete per field**: a cleared value emits `<s> <p> *` in the same mutation |
| ☐ P4-3 | events | Blank-node labelling for new nodes | **[V]** Byte-parity with Go **not** required (assessment §2.2); only batch-local uniqueness and correct `blankToID` round-trip |
| ☐ P4-4 | links | Port `linkEvent1IDsQuery`, `linkDetailsQuery`, delete-then-write, `cleanupMissingLinks` | **Chunk by `event1_id`, never by link row** |
| ☐ P4-5 | links | Facet construction | Only non-empty values emitted |
| ☐ P4-6 | sync_time | Port stamping, batched 200 rows per `UPDATE` | **Single final step, after all phases** — the restartability guarantee |
| ☐ P4-7 | matviews | **Not ported** **[C]** — the matview refresh left the ETL (Go `materialized_views.go` deleted); a separate Step Functions state owns it (P2-9) | No `REFRESH MATERIALIZED VIEW` anywhere in the Python repository |
| ☐ P4-8 | — | **No self-gate** **[C]**; entry point runs removal → events → links → sync_time, then exits | — |
| ☐ P4-9 | — | Port L3 SQL tests unchanged against the same fixture | — |

### 7.2 Local stack & end-to-end (L4)

The assessment's L4 layer, executed locally. This is the first time the Python ETL writes to a real Dgraph.

| ID | Task |
|---|---|
| ☐ P4-10 | **Local stack as code** — a `docker compose` (or equivalent) definition bringing up **Postgres** and a **Dgraph zero + alpha**, alpha version pinned to INV-9. One documented command up, one down, no manual steps |
| ☐ P4-11 | Seed Postgres from the **P1-6/P1-7 synthetic fixture** — the same script, not a second copy. Apply `schema.dql` to the local alpha using the **INV-5 procedure**, so the procedure itself is exercised locally before it is relied on in Staging |
| ☐ P4-12 | Run the Python ETL from source against the local stack (`SECRETS_SOURCE=env`); assert graph state after each of the four phases |
| ☐ P4-13 | Exercise the failure paths locally: kill the process mid-`events`; stop the alpha mid-run; present an unreachable Postgres. Assert **non-zero exit**, a diagnosable log line, and **zero `sync_time` rows stamped** |
| ☐ P4-14 | **Synthetic-data and credential guards — in the regular test suite** **[C]** (D4). Ordinary tests in the default suite (`pytest` in the Python repo, `go test` in the Go repo), **not** pipeline steps — so they hold under PD-10 with no CI dependency. Each must run **fully offline**: no AWS call, no network, no real credential, no environment-specific input. Four checks: **(a)** no tracked file matches database-dump or bulk-extract patterns (`*.sql`, `*.dump`, `*.csv`, `*.rdf.gz` outside fixtures); **(b)** no tracked `.env` or credential-bearing file, and no high-entropy literal in tracked source; **(c)** every fixture row carries the synthetic marker, and none matches a real-data heuristic; **(d)** the §8.5 key-contract test — the loader's required-key list equals the seven `db_*` names, asserted against the documented list, **never** against Secrets Manager |

### Deliverables
- Complete Python incremental ETL
- L2 + L3 test suites passing in Python
- Reproducible local stack (Postgres + Dgraph) with a one-command bring-up
- L4 end-to-end evidence for all seven scenarios

### Acceptance criteria
- ☐ **Every P1 golden file reproduced byte-for-byte** — the primary equivalence gate
- ☐ L3 queries return rowsets identical to Go's against the shared fixture
- ☐ All 7 scenarios pass **end-to-end against the local Postgres + Dgraph**, not only against golden files
- ☐ A deliberately interrupted run leaves **zero** rows stamped and reprocesses cleanly on retry
- ☐ Every failure injected in P4-13 produces a non-zero exit and a log line naming the failing phase
- ☐ No `traceability_log_daily` reference anywhere in the codebase **[C]**
- ☐ No `REFRESH MATERIALIZED VIEW` and no `mv_events` / `monitoring_cache_shipments` / `mv_shipment_list` reference anywhere in the codebase **[C]**
- ☐ **Guard tests (P4-14) green as part of the default test command** — no separate invocation, no flag to remember
- ☐ Guards pass with **networking disabled and no AWS credentials present**, proving they depend on neither
- ☐ Each of the four checks **demonstrated to fail** against a planted fixture (negative test per check, not one blanket test)
- ☐ The key-contract check (d) fails when the loader's required-key list and the §8.5 table diverge

---

## 8. P5 — Verification Harness, Container & Deployment Infrastructure · *Local → Staging*

**Objective.** Build the independent oracle that replaces live differential testing (assessment §4.1), make the container ECS-ready, and stand up the Staging deployment against the provided resources.

**Dependencies.** P4 (acceptance), P0-7, P0-10 … P0-14, and a **populated** `stg/etl-dgraph/creds` (§8.5).

### 8.1 Verification harness *(Local)*

| ID | Task |
|---|---|
| ☐ P5-1 | Build a **reusable** harness: after a run, re-query Postgres for the changeset and assert Dgraph matches — field values, links, facets, `sync_time` stamped |
| ☐ P5-2 | **[R] Derive assertions from the field mapping, not from the Python implementation** — otherwise it validates the port against itself |
| ☐ P5-3 | Instrument the metrics required by assessment §4.3: ID-set and `uidMap` cardinality, peak RSS, per-phase wall-clock, Dgraph mutation/retry/abort counts |
| ☐ P5-4 | Package the harness to run as a standalone Production monitoring check |
| ☐ P5-5 | Emit a machine-readable per-run verdict (pass/fail + metrics) to stdout — this is the evidence source for PD-1 and PD-2, and must survive being read out of CloudWatch Logs |
| ☐ P5-6 | **Validate the harness locally first** — run it against the P4-12 local run, and against a deliberately corrupted local graph (negative test) |

### 8.2 Container & image *(Local)*

| ID | Task |
|---|---|
| ☐ P5-7 | Dockerfile — **no `.env`** **[V]** (contrast `deployment/dockerfiles/Dockerfile-stg:34`, which `COPY .env`), no tzdata, non-secret config only, pinned dependency versions, non-root user, no build toolchain in the runtime layer |
| ☐ P5-8 | **Architecture match** — build `linux/amd64` unless INV-10 shows the task definition declares `ARM64`. **[V]** The Go images are `GOARCH=amd64`; a mismatched Python image fails at task start, not at build |
| ☐ P5-9 | Drop the meaningless `EXPOSE 8080` **[V]** carried by the Go images — this is a batch task with no listener |
| ☐ P5-10 | **Run the built image locally against the local stack** (§7.2) — same entrypoint, config via env vars, `SECRETS_SOURCE=env`. Confirm full pipeline success and **exit 0** |
| ☐ P5-11 | Repeat the P4-13 failure injections **through the container**; confirm **non-zero exit** and that logs reach stdout before the process dies |

### 8.3 ECS Fargate task readiness *(this plan's responsibility — PD-4)*

Everything required for the task to run correctly **when triggered**. Workflow placement is excluded by PD-4.

| ID | Area | Task |
|---|---|---|
| ☐ P5-12 | **Exit status** | Exit **0** only when all four phases completed and `sync_time` was stamped. Any unhandled exception, failed secret fetch, unreachable dependency, or partial phase → **non-zero**. **[R]** Distinct codes for *config/secret failure* vs *data/graph failure* so an alarm can tell them apart |
| ☐ P5-13 | **Signal handling** | Handle `SIGTERM` (ECS task stop, Step Functions timeout/abort): stop accepting new batches, **never stamp `sync_time`**, exit non-zero promptly. Verify the task definition's `stopTimeout` allows it |
| ☐ P5-14 | **Logging** | `awslogs` driver → log group, region `ap-southeast-3`, stream prefix. One JSON object per line to stdout; `PYTHONUNBUFFERED=1`; **no secret value ever logged**. Log explicit phase-start/phase-end markers so clean-cycle criterion **A2** is machine-checkable, and a run identifier correlating the ETL and harness output |
| ☐ P5-15 | **Secrets access** | Runtime `boto3` fetch of `stg/etl-dgraph/creds` from `ap-southeast-3`, **once at startup**, against the **§8.5 key structure**. Validate every required key is present and fail fast with a distinct exit code naming the **missing keys** — never their values. **[R]** Retry transient `GetSecretValue` throttling, not `AccessDenied` |
| ☐ P5-16 | **IAM** | Task-role policy, now fully determined — see §8.5 for the exact document. One statement, one action, one resource ARN; **no `kms:Decrypt`** **[C]** D1. Separate secrets per environment with distinct ARNs, so Staging cannot read Production credentials |
| ☐ P5-17 | **Configuration** | Non-secret tunables (batch sizes, concurrency, chunk size, log level, **Dgraph alpha endpoints** per §8.5 A3) via the task definition's `environment` block or the baked config file. **No `APP_MODE`** — the Python ETL is single-purpose; carrying the Go two-mode switch forward would be a silent foot-gun. **No replica DB variables** — **[V]** unused (§8.5) |
| ☐ P5-18 | **Networking** | Confirm from INV-6/INV-10 that the task's subnets and security group reach: Postgres, the Dgraph alphas, Secrets Manager, ECR + ECR-DKR + S3 (image pull), and CloudWatch Logs. **[R]** A private subnet without NAT needs interface endpoints for all of them, not only Secrets Manager — the single most common Fargate task-start failure |
| ☐ P5-19 | **Sizing** | CPU/memory from the §6 projection (~250 MB peak RSS → **[R]** ≥1 GB), refined by P6 measurements. Default 20 GB ephemeral storage is ample — the Python ETL writes no files |
| ☐ P5-20 | **Task definition revision** | Register a **new revision** of `GAR-Stg-Trace-Task-ETL-Dgraph` **manually, by the §8.6 runbook** **[C]** (PD-6, PD-7): derived from the latest applicable revision, carrying the Python image **by digest**, entrypoint, roles, log configuration and environment. Record the revision number and image digest and hand them to the owner — **no Step Functions change** |
| ☐ P5-21 | **Standalone invocation test** | Run the registered revision via `aws ecs run-task` **directly**, outside any workflow, and verify: task reaches `RUNNING`, secrets resolve, all four phases log, task exits 0, logs land in CloudWatch, and `stoppedReason`/`exitCode` are correct. Repeat with an injected failure and confirm a non-zero `exitCode` surfaces on the task description. **This is the acceptance evidence for PD-4's half of the boundary** |

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
| **S1** | The existing image is the **Go** ETL: distroless, static binary entrypoint, `COPY .env` into the image | **[V]** `deployment/dockerfiles/Dockerfile-stg:34,39` | A new Python image must be built and pushed under `dgraph-incremental`. **Revision `:1` cannot run the Python ETL** — image, entrypoint, roles, log configuration and environment all change |
| **S2** | **CI never pushes `:latest`.** The Azure pipeline tags images `$(Build.BuildId)` only, yet **[C]** the Go ETL runs from `:latest` | **[V]** `azure-pipelines.yml:9-11`, `templates/publish-template.yml` (`docker push …:${ImageTag}`) | **Resolved for the Python path by PD-5** — the two implementations occupy separate tags, so neither can overwrite the other's artefact. **Residual, recorded not fixed:** the deployed Go `:latest` was **not produced by CI** **[V]**, so it has no pipeline provenance and cannot be traced to a commit **[?]**. This is why §8.6 step 2 anchors rollback to the **digest**, not the tag |
| **S3** | `dgraph-incremental` is a **mutable** tag across Python builds — each push replaces the previous image under the same tag | PD-5 | The tag cannot identify *which* build a Staging cycle ran. **[R]** Record the **image digest** in the cycle log (§9.2) and in the task-definition revision's description; resolve it with `aws ecr describe-images --image-ids imageTag=dgraph-incremental`. This satisfies PD-1's "same artefact across all 5 cycles" requirement without introducing tag automation |
| **S4** | The Go task runs with credentials baked in the image, so its **task role may carry no `secretsmanager:GetSecretValue`** | assessment §7.1 | P5-16 is a **prerequisite**, not a refinement; verify against INV-10 before the first Staging run |
| **S5** | `APP_MODE` and the Go-shaped env block in the existing revision have no meaning for the Python ETL | **[V]** `build.yml` env list | The new revision's `environment` block is authored fresh (P5-17), not inherited |
| **S6** | *Closed.* The secret was empty when §8.5 was written; it is now **populated** with the seven `db_*` keys **[C]**, and §8.5 records the key names as populated rather than as proposed | provided | None outstanding. P5-15 still fails fast on a missing key, by design |

**Requires verification before P5.4.**

| # | Item | Investigation |
|---|---|---|
| ☐ V-a | Full JSON of the latest ACTIVE revision — `cpu`, `memory`, `runtimePlatform.cpuArchitecture`, `taskRoleArn`, `executionRoleArn`, `networkMode`, `logConfiguration`, `environment`, `secrets`, `ephemeralStorage`, `stopTimeout` | INV-10 (P0-11), via §8.6 step 1 |
| ☑ V-b | *Resolved.* ARN `…:secret:stg/etl-dgraph/creds-0mxXQ4` (§8.4); encryption **[C]** D1, AWS-managed `aws/secretsmanager` | — |
| ☑ V-c | *Resolved.* Secret populated with the seven `db_*` keys (§8.5). Values unverified by this plan — first proven at P5-21 | — |
| ☐ V-d | **ECR tag immutability** on `gar-stg-trace-ecr-dgraph-etl`. **[C]** D3 confirms only that `dgraph-incremental` is **unused**, so the *first* push will succeed either way; it says nothing about the repository's immutability setting. If immutability is **enabled**, the **second** push to that tag fails — which is the first image fix or the P6-7 resize, not the initial deploy. Verify before the second push, not before the first | INV-12 (P0-13) |
| ☐ V-i | **ECR lifecycle policy** on the same repository — confirm it cannot expire the **Go rollback image** (P7-3). Not covered by D3 | INV-12 (P0-13) |
| ☐ V-e | The subnets and security group the task runs in, and whether they reach Postgres, Dgraph, Secrets Manager, ECR and CloudWatch Logs | INV-6 (P0-7) + INV-10 |
| — V-f | *Withdrawn.* **[C]** PD-9 — the family name is the intended reference, and workflow integration is **[O]** owner-owned and out of scope | — |
| ☐ V-g | Dgraph **ACL** posture on the Staging cluster. **[C]** C2: proceed assuming it is **not** in use; this remains an **unverified assumption** (§8.5 A4), carried as a risk rather than a blocker | INV-13 (P0-14) — **[R]** opportunistic |
| — V-h | *Resolved.* **[C]** PD-8 — `sslmode=disable`; TLS is not in use | — |

### 8.4.1 Runtime configuration inventory — Staging

Closes the gap that §8.4's tables did not cover: **every value the task needs at runtime, where it comes from today, and whether it is known.** Credentials are §8.5; this table is everything else.

| Value | Purpose | Current source (Go) | Status | Blocks |
|---|---|---|---|---|
| **`DGRAPH_ALPHAS`** | Alpha endpoints, comma-separated. **[V]** `app.yaml:35` defaults to `[]` and `dgraph.go:26` **errors immediately** on an empty list — there is no usable default | **[V]** baked `.env`, from the `trace-dgraph-etl-stg` variable group (`build.yml:42`) — **not necessarily in the task definition** | **⏳ Unknown — INV-14** | **P5-17, P5-21** |
| `DGRAPH_SSL_MODE` | **[V]** No-op — all branches use `insecure.NewCredentials()` | `.env` / `app.yaml` | Known; `disable` | — |
| `DGRAPH_TIMEOUT_SECOND`, `DGRAPH_TRANSACTION_TIMEOUT_SECOND`, `DGRAPH_MAX_RETRIES` | Client timeouts and retry ceiling | `app.yaml` defaults (30 / 0 / 100) | Known — defaults usable | — |
| `INCREMENTAL_FETCH_CHUNK_SIZE` and the per-phase batch/concurrency knobs | Throughput tuning | `app.yaml` defaults (1000; batches 10–100, concurrency 4) | Known — defaults usable | — |
| `LOGGER_LEVEL` | Log verbosity | `app.yaml` (`debug`) | Known — **[R]** `info` in Staging | — |
| `DB_CLEAN_*` pool/timeout knobs | Pool sizing, connect timeout, charset | `app.yaml` defaults | Known — defaults usable | — |
| `DB_CLEAN_*` coordinates and credentials | Postgres connection | **[C]** the secret (§8.5), seven `db_*` keys | **Resolved** | — |
| CloudWatch log group | `awslogs` destination | existing Go task definition | **☐ V-a** | P5-14, P5-20 |
| `APP_MODE`, `INITIAL_*`, `DB_CLEAN_REPLICA_*`, `AWS_S3_*`, `DGRAPH_USERNAME`/`PASSWORD`/`BEARER_TOKEN` | — | `.env` | **[V]** Not needed — excluded (§8.5) | — |

**INV-14 — how to obtain `DGRAPH_ALPHAS` without exposing a secret.** The endpoint list is **not a credential**: it is a set of host:port addresses (alpha gRPC, conventionally `:9080`). It is nonetheless **[R]** recorded in the task definition and the deployment log, not inline in this plan. Sources, least-exposing first:

1. **Ask the Dgraph cluster owner**, or read the alpha hosts from the provisioning scripts (`scripts/setup_dgraph_alpha.sh`). Touches no secret at all.
2. **ADO variable group `trace-dgraph-etl-stg`** → `DGRAPH_ALPHAS`. Visible if the variable is not marked secret; reading one variable exposes nothing else.
3. **The running task definition** — `describe-task-definition` → `containerDefinitions[0].environment` (V-a). **[A]** Likely absent, since the Go task reads it from the baked `.env`.
4. **Last resort — extract one line from the image.** The image is distroless with no shell, so `docker run` cannot inspect it; use `docker create` + `docker cp`, and **pipe through a filter so the credential-bearing file is never written to disk or displayed**:
   `docker cp <container>:/app/.env - | tar -xO | grep '^DGRAPH_ALPHAS='`
   Then remove the container. **Never `cat` that file** — **[V]** it holds `DB_CLEAN_PASSWORD`, `DGRAPH_PASSWORD`, `DGRAPH_BEARER_TOKEN` and the AWS keys. Prefer options 1–3.

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
| **A4** | **[A] Unverified assumption — explicitly not a confirmed fact** **[C]** per C2 | **Dgraph Staging does not use ACL.** The supporting observation is **[V]** that the Go client connects with `insecure.NewCredentials()` and never logs in, so an enforced ACL would already break the current Go task. That is an inference from the client's behaviour, **not an observation of the cluster**, and no one has checked the cluster | If ACL is in fact enabled, the Python client must perform a login the Go client never did — a **code change in P3-8**, plus `dgraph_username` / `dgraph_password` added to this secret. Cheap in P3; expensive if first discovered at P5-21. Tracked as INV-13 and as a Medium risk (§12) |
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
| ☐ P5-22 | **No new ECR repository for Staging** **[C]** PD-5 — the Python image shares `gar-stg-trace-ecr-dgraph-etl` under the `dgraph-incremental` tag, which **[C]** D3 confirms is **unused**, so the first push is unblocked. Before the **second** push, verify tag immutability is disabled (V-d); before P7-3, verify the lifecycle policy cannot expire the Go image (V-i). **[?]** Whether Production repeats this sharing or uses a dedicated repository is **open** — §15.3 |
| ☐ P5-23 | Apply INV-6: provision the Secrets Manager (and, if needed, ECR/Logs) VPC endpoints |
| — P5-24 | **Moved out of P5.** **[C]** PD-10 — pipeline work is deferred until Staging completes; see §15. For Staging the image is built and pushed manually, which P5-21 already proves end to end. The constraints that will apply whenever the pipeline is built are recorded in §15.5 |
| ☐ P5-25 | Mirror the configuration for Production **before** P7 — Production secret with identical keys and a distinct ARN, and the equivalent image tag in the **Production account's** registry (**[C]** PD-11: same tag convention, separate accounts, §15.4) — so cutover is a task-definition swap and not a first-time build. **[?]** Which Production repository holds it is open (F4, §15.3) |

### Deliverables
- Verification harness, runnable standalone, emitting machine-readable verdicts
- Python image, locally proven against the local stack, published as `gar-stg-trace-ecr-dgraph-etl:dgraph-incremental`
- New Staging task-definition revision, registered manually per §8.6 and proven by a standalone `run-task`
- Secret structure specification (§8.5), confirmed and populated by the owner
- IAM roles, networking, deployment log

### Acceptance criteria
- ☐ Harness detects a **deliberately injected** graph discrepancy (negative test — proves it is not vacuously passing), verified locally first
- ☐ Image builds and runs the full pipeline **locally** against the local stack, exit 0
- ☐ Image runs on Fargate via standalone `run-task` (P5-21); secrets resolve from the populated `stg/etl-dgraph/creds`; **no credentials in the image**
- ☐ An injected failure surfaces as a non-zero `exitCode` on the stopped task, with the failing phase named in CloudWatch Logs
- ☐ A missing or malformed secret key fails at startup with the config/secret exit code and names the missing key, not its value
- ☐ The Go image and its last Go-based task-definition revision are **both still intact** after the Python image is published (PD-5, §8.6 step 2)
- ☐ The §8.6 pre-deployment checklist passes against the registered revision, and the deployment log entry exists
- ☐ IAM verified least-privilege: the Staging role **cannot** read the Production secret
- ☐ §8.4's **V-a, V-b, V-c, V-e, V-g** resolved (V-d and V-i are later gates, per §14.2 E2/E3)

---

## 9. P6 — Staging Validation · *Staging*

**Objective.** Validate integration **and scale** on Staging, which **[C]** is representative of Production.

**Dependencies.** P5 (acceptance), P2 (acceptance) **[O]**, Local exit criteria (§2.1) recorded as met.

**Exit threshold (PD-1) [C]:** **≥5 consecutive clean daily cycles.**

### 9.1 Clean-cycle definition **[C]**

A cycle counts as **clean** only when **all three dimensions pass**. Any single failure disqualifies the cycle.

| Dim. | Criterion | Measurement |
|---|---|---|
| **A1** | ETL task exits 0, no Step Functions timeout | Execution status |
| **A2** | All four phases completed: removal → events → links → sync_time | Phase-completion log (P5-14 markers) |
| **A3** | Completed **without needing a retry** | Retry count = 0 |
| **A4** | The downstream matview-refresh state (P2-9) completed; the three views are fresh | Step Functions execution + view timestamps. **[?]** Presupposes the state exists — **INV-15**. If it does not, A4 is **unsatisfiable** and §9.2's *Hold* rule would stall the count indefinitely rather than fail it; resolve INV-15 before P6-1 |
| **B1** | Verification harness passes with **zero** discrepancies on its sample | P5-5 verdict |
| **B2** | No `sync_time IS NULL` residue for rows in the processed changeset | Post-run query |
| **B3** | Every valid link in the changeset present in the graph with correct facets | Harness assertion |
| **B4** | Every invalidated event in the changeset **absent** from the graph | Harness assertion |
| **C1** | Peak RSS within the assessment §6 envelope; no OOM | Container metrics |
| **C2** | Wall-clock ≤ 70% of `TimeoutSeconds` **[R]** (margin, not a cliff) | Run duration |
| **C3** | ID cardinality within the range established by INV-8; **not** >1M | P5-3 metric |
| **C4** | Dgraph retry/abort counts within the baseline from the first clean run | P5-3 metric |
| **C5** | Zero unhandled exceptions or `ERROR`-level logs | Log scan |

### 9.2 Counter rules **[C]**

The distinction that matters: a **qualifying failure** indicates a defect in the Python ETL and **resets** the counter; a **non-qualifying** event means no valid observation occurred, so the counter **holds** and the cycle is simply excluded.

| Event | Counter effect |
|---|---|
| ETL task failure in any phase | **Reset to 0** |
| Parity/correctness mismatch (B1–B4), unresolved | **Reset to 0** |
| Operational-health breach (C1–C5) | **Reset to 0** |
| Run succeeded only after retry, root cause **in the ETL** | **Reset to 0** |
| **Any code change to the Python ETL** | **Reset to 0** — prior cycles no longer evidence the deployed artifact |
| **Any new image or task-definition revision deployed to Staging** | **Reset to 0** — the artefact under observation changed. **[R]** Because `dgraph-incremental` is a mutable tag (§8.4 S3), the comparison is made on the **image digest** recorded in the §8.6 deployment log, not the tag |
| Mismatch traced to a **harness defect**, ETL proven correct | **Hold**; cycle excluded; re-verify after the harness fix |
| Run succeeded after retry, root cause **external and transient** (verified) | **Hold**; cycle excluded |
| No ETL run because Traceability failed and the workflow halted — expected behaviour per assessment decision 11 | **Hold**; cycle excluded |
| External platform outage prevented the run | **Hold**; cycle excluded. **[R]** Investigate if it recurs |
| Matview-refresh state (A4) failed while the ETL itself was clean | **Hold**; cycle excluded — a separate state, not an ETL defect; track and fix independently |

**[R]** Record every cycle's verdict, counter state, **and the image digest it ran** in a run log, so the exit decision rests on evidence rather than recollection.

### 9.3 Tasks

| ID | Task |
|---|---|
| ☐ P6-1 | Deploy the Python revision registered in P5-20 (§8.6). **[C]** The Go image stays in ECR untouched (PD-5) and `td-go-rollback.json` (§8.6 step 2) is committed as the rollback anchor. **[C]** PD-9: the family name resolves to this revision on the next execution |
| ☐ P6-2 | Run daily cycles under the workflow; the harness runs after each |
| ☐ P6-3 | Capture §4.3 metrics every run; compare against §6 projections **and against the local baseline from §2.1** |
| ☐ P6-4 | Validate `TimeoutSeconds` against observed durations; hand the figure to the owner **[O]** |
| ☐ P6-5 | Exercise retry: force a mid-run failure and confirm clean reprocessing. **[R]** Run this deliberately *before* starting the clean-cycle count, so the test does not reset it |
| ☐ P6-6 | **[R] Optional** `sync_time` replay check (assessment §4.2). **Note the documented limitation:** the removal path cannot be cleanly A/B'd this way — covered by P1-5 and scenario 3 instead |
| ☐ P6-7 | Right-size the Fargate task from measured peak RSS; register the resized revision **before** the clean-cycle count begins, not during it (§8.6) |
| ☐ P6-8 | Maintain the cycle log (§9.2) |

### 9.4 Acceptance criteria

- ☐ **≥5 consecutive clean daily cycles** **[C]**, every dimension in §9.1 passing, all on the **same image digest and task-definition revision**
- ☐ Cycle log complete, each entry with verdict, metrics, image digest, revision number, and counter state
- ☐ Final `TimeoutSeconds` and task sizing set from measured values, not estimates
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
| **INV-1** | Step Functions `.sync` integration pattern **[O]** | P0-2 | **P2, and the premise of assessment decision 11** |
| **INV-2** | Halt-on-failure, retry, timeout, concurrency guard, alarms **[O]** | P0-3 | P2 |
| **INV-3** | pydgraph `set_nquads` + `del_nquads` in one mutation | P0-4 | P4-2 set-or-delete semantics |
| **INV-4** | pydgraph gRPC gzip via channel options | P0-5 | P3-8 (performance only, not correctness) |
| **INV-5** | Schema-application procedure; deployed-vs-file match | P0-6 | P3-2, P4-11, P7-10 |
| **INV-6** | VPC endpoint requirements (Secrets Manager, ECR, Logs) | P0-7 | P5-18, P5-23 |
| **INV-7** | Bulk-loader invocation location | P0-8 | P7-10 |
| **INV-8** | Measured ID cardinality | P0-9 | Confirms assessment decision 6; sets the C3 threshold; triggers §6.1 review if >1M |
| **INV-9** | Deployed Dgraph alpha version | P0-10 | P4-10 local stack parity; P3-8 pydgraph pin |
| **INV-10** | Full JSON of the **latest ACTIVE** `GAR-Stg-Trace-Task-ETL-Dgraph` revision (§8.4 V-a, §8.6 step 1) | P0-11 | P5-8, P5-16, P5-18, P5-20 |
| ~~**INV-11**~~ | *Closed.* `stg/etl-dgraph/creds` — ARN, AWS-managed KMS key, and population with the seven `db_*` keys all settled (§8.5) | P0-12 | Nothing |
| **INV-12** | ECR tag immutability (V-d) and lifecycle protection of the Go image (V-i). Tag **availability** is **[C]** D3 | P0-13 | **Not the first push** — the second image push (P6 iterations, P6-7) and P7-3 |
| **INV-13** | Dgraph ACL posture (§8.5 A4) — **[R]** opportunistic, **not a gate** **[C]** per C2 | P0-14 | Nothing. If it turns out enabled, it is a P3-8 code change; carried as a Medium risk (§12) |
| **INV-14** | Staging `DGRAPH_ALPHAS` endpoint list (§8.4.1) | P0-15 | **P5-17, P5-21** — no usable default exists **[V]** |
| **INV-15** | Whether a separate downstream matview-refresh state exists **[O]** | P0-16 | **P2-9 scope, P2-11 sequencing, and clean-cycle criterion A4** |

---

## 12. Risk → mitigation map

| Risk | Severity | Mitigated by |
|---|---|---|
| Ordering void if `.sync` absent | Medium | INV-1 (P0-2) **[O]**, P2-2 |
| Schema predicate drift at bulk reload | Medium | P7-11 |
| Out-of-band invocation races Traceability | Medium | P2-8 **[O]** |
| Views go stale after the refresh leaves the ETL | Medium | P2-9, P2-10, P2-11 (sequencing guard), clean-cycle criterion A4 |
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
| **Real data leaks into local development** | Medium | PD-3; synthetic-only fixture (P1-6); CI guard (P4-14) |
| Memory blow-up | Low | INV-8 (P0-9), criterion C1/C3, local RSS baseline (§2.1), P6-7 |
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

All 15 assessment decisions are carried unchanged except **#15 (rename)**, reopened as **H1**; PD-1 … PD-11 are carried unchanged.

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

### 14.2 Outstanding items

**No owner decisions or inputs remain outstanding.** Every item below is closed:

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

Two further decisions are **open and not deferred** — both gate work that is otherwise ready to start:

| # | Open decision | Gates | Status |
|---|---|---|---|
| **H1** | **Repository strategy** — the owner proposes creating **two new repositories**, `ETL-Dgraph-Initial` and `ETL-Dgraph-Incremental`, rather than renaming. This **modifies assessment decision 15** (rename), so it is recorded as open rather than applied | **P0-1, P1-9** | **[?]** Assessed, not decided. The provenance-critical point: a new repository **seeded by `git push` from this clone preserves every commit**; one seeded by copying files into a fresh `git init` **does not**, and P1-9's SHA citations would then be unresolvable |
| **H2** | **INV-15** — does a separate downstream matview-refresh state exist? **[O]** | **P2-9 scope, P2-11, A4** | **[?]** Newly opened by **[C]** D2; see §5 |

Two items remain flagged as **[R] recommendations rather than confirmed decisions**, each safe to adopt or drop without reopening a decision:

| # | Recommendation | Where |
|---|---|---|
| **R1** | Clean-cycle criterion **C2** — wall-clock ≤70% of `TimeoutSeconds` as a margin threshold | §9.1 |
| **R2** | Soak safeguard — **≥10 successful cycles** within the 14 calendar days, so a low-activity window cannot satisfy PD-2 vacuously | §10.2, G3 |

Everything else outstanding is an investigation task (§11) with a named owner-phase, requiring no decision.

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

This **supersedes `REFACTOR_ASSESSMENT.md` §2.4 R4**, which recorded the branch triggers (`gar-dev`, `gar-stg`, `gar-prd-1`) as unaffected by the repository rename. They are affected — by this change, not by the rename.

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

