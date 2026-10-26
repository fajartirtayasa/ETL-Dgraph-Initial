# Implementation plan: remove Incremental from ETL-Dgraph-Initial

**Status: authorised for execution (2026-10-26).** The owner confirmed every item in
[§2.2](#22-pending-decisions-require-owner-confirmation) exactly as recommended and explicitly authorised execution
on 26 October 2026 (C19). Execution proceeds stage by stage, behind the gates of §3 and C18.

**Stage 0 (read-only audit) is done.** It found two blockers, both recorded as decisions below: Initial's working
tree carried two files that §4.1 did not list (C16), and the Python test containers have no `git`, which
`tests/test_retry.py` needs after the removal (C17). Stage 1 and Stage 9 stay gated (C18). The only edits made so far
are this file and the Python test-container files named in C17; nothing else has been committed or changed.

Repositories, as sibling folders:

| Short name | Folder | Role |
|---|---|---|
| **Initial** | `ETL-Dgraph-Initial` | Go. Bulk RDF export (`initial` mode). This repository |
| **Python** | `ETL-Dgraph-Incremental` | Python port of the incremental ETL; owns the canonical `schema.dql` |
| **Dgraph** | `Dgraph` | Holds the authoritative `REFACTOR_PLAN.md` / `REFACTOR_ASSESSMENT.md` |

---

## 1. Scope

**In scope**

- Remove every Incremental-only component from Initial: `internal/etl/incremental/`, `pkg/dgraph/`, the two-mode
  dispatch, the Dgraph and incremental configuration, the parity golden corpus and the Go parity tests.
- Make Initial accept only `APP_MODE=initial`.
- Replace the ECS deployment path with a local Docker build of a standalone `linux/arm64` binary.
- Keep the synthetic fixture and the canonical-schema mirror, each protected by an automated check.
- Update documentation in Initial, the Python repository's outdated operational references, and the live plan in
  the Dgraph repository.

**Out of scope**

- Any change to Initial's export semantics (queries, RDF output, blank-node IDs, chunking).
- Unused pre-existing code that is not Incremental-specific: `internal/etl/common/rdf_writer.go`, `pkg/aws`,
  `pkg/database/migration.go`, `db_clean_replica`, `pkg/validator`, `pkg/httpx`, most of `pkg/util`.
- Python ETL behaviour, its deployment, the ECS task definitions and the Go rollback image in ECR.
- Open operational items in the live plan: INV-7 H6 (where Initial runs, how `initial_imports/` reaches the bulk
  host) and H3 (memory and disk at ~35 M nodes on 4–8 GiB arm64 hosts).
- Pushing to any remote.

---

## 2. Decisions

### 2.1 Confirmed decisions

| ID | Decision |
|---|---|
| **C1** | Remove Incremental from Initial now, before the P7 decommission gate. The Go hotfix path is given up; the Go image in ECR remains the rollback artefact |
| **C2** | Initial is **not deployed to ECS**. The output is a standalone `linux/arm64` binary named `traceability-dgraph-etl`, saved under `out/`, built locally through a Go container. The target is Linux arm64 on EC2 |
| **C3** | Remove `deployment/` (every function in it is ECS-only) and the root `Dockerfile` (it builds an image, not a binary) |
| **C4** | `APP_MODE`: trim whitespace, ignore case, accept only `initial`. Empty or any other value is rejected with a clear error **before** connecting to PostgreSQL |
| **C5** | Keep `parity/fixture/` and its generator in Initial. The fixture is frozen for compatibility with the Python repository, which reads it in place and pins it in `tests/fixture.lock`. Add a checksum guard in Initial |
| **C6** | Delete `parity/golden/` (verified unused by Initial at runtime and by Python, which holds a byte-for-byte copy). Remove the hard-coded 28-golden check from `parity/guards/guards_test.go`; keep every security guard |
| **C7** | Keep `schema.dql` in Initial, **byte-for-byte identical** to Python's. An automated test compares Initial's emitted predicates and scalar types with **Python's** schema, exactly, in both directions, and **fails** (never skips) if Python's schema is unavailable |
| **C8** | The bulk-load runbook uses Python's `schema.dql` |
| **C9** | Rename `scripts/` to `setup/` with `git mv`. The scripts are unused (they describe x86 `c5d` hosts, three replicated zeros and the amd64 `latest` Dgraph binary; the live Staging hosts are arm64 `c6gd`, one zero, one alpha, v25.3.1). Preserve the scripts and their history |
| **C10** | Delete the stale `REFACTOR_PLAN.md` and `REFACTOR_ASSESSMENT.md` copies in Initial. The Dgraph repository's copies are authoritative |
| **C11** | Keep useful historical documentation with clear historical labels; remove or update obsolete operational instructions. Keep `command.txt` while Python relies on it for field mappings |
| **C12** | Record the decisions in the live plan (Dgraph repository). Update outdated operational references in the Python repository |
| **C13** | Commit the existing pending changes in all three repositories to each `main` first, separate from the cleanup. Cleanup happens on separate branches; merge to `main` only after review and approval. Never discard or mix unrelated changes |
| **C14** | The 1.6 MB diagram PNG is included in the Python commit and stays in Git history permanently |
| **C15** | Docker stays inactive until after the plan is executed. Build and test happen later through Docker, targeting `linux/arm64` |
| **C16** | Owner decision (after Stage 0). The pending change in `Dgraph/REFACTOR_PLAN.md` is preserved and committed as planned. Neither Initial file loses content. Initial's `REFACTOR_PLAN.md` has a pending change that is byte-identical to Dgraph's (sha256 `cd05e357…e43f`); it is committed in Phase 0 **after** Dgraph's commit and only if the hashes still match. Initial's `REFACTOR_ASSESSMENT.md` has **no** content change (its working copy equals `HEAD` and equals Dgraph's `HEAD` copy, sha256 `750fbf39…3f8a`; `git status` shows it modified only because the index holds a stale stat entry from a CRLF checkout): nothing is committed for it. Phase 0's exit criterion is adjusted for both (§4.2 step 5), and Commit 8 deletes them only after V18 proves both copies exist, unchanged, in Dgraph's `main` |
| **C17** | Owner decision (after Stage 0). `git` is added to the Python test containers used by `scripts/test.sh` and `scripts/demo_guards_offline.sh`, so `tests/test_retry.py` can run `git show b487294:pkg/dgraph/dgraph.go` (P12). A new `scripts/test.Dockerfile` builds the image from the **same digest-pinned** `python:3.12.3-slim` as the production `Dockerfile`, adds `git` with `--no-install-recommends` and removes the apt lists. `scripts/test_image.sh` (sourced by both scripts) builds it once, tags it `etl-incr-test:<first 12 hex of the Dockerfile's sha256>` (a changed Dockerfile builds a new image, an unchanged one is reused) and sets `safe.directory` for **`/work/ETL-Dgraph-Initial` only**, through `GIT_CONFIG_COUNT/KEY_0/VALUE_0` (no global git config, no `*`). The offline demo builds the image in its network-on setup step, never under `--network none`, and fails if the container cannot read `b487294:pkg/dgraph/dgraph.go`. The Debian `git` version is not pinned (it follows the base image's repository on the build day; `git show` is stable across it). Docker was not run to make this change (C15): the proof is V19 |
| **C18** | Owner decision (after Stage 0). **Stage 1 does not start** until C16–C18 are recorded here and the working trees match §4.1 exactly (re-checked with `git status --short` in all three repositories). **Stage 9 stays blocked** until its own prerequisite is met: git proven to work inside both Python test containers (V19) |
| **C19** | Owner confirmations, 2026-10-26 (the Stage 1 owner block). **O2:** P1–P20 confirmed exactly as recommended in §2.2. **§3 item 2:** execution explicitly authorised by the repository owner on 26 October 2026. **O1:** the stage mapping in the prompts file is accepted. **O3:** this file is committed under P20 *after* the owner's edit recording these confirmations (this row, the status line and the §2.2 note) |
| **C20** | Owner decision, 2026-10-26: every commit made under this plan, in all three repositories, ends with exactly `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`. This is an explicit exception to the stage prompts' stop condition for repositories whose recent history shows more than one trailer variant (Python and Dgraph also show a Sonnet 5.5 variant). Historical commits are not rewritten |

### 2.2 Pending decisions (require owner confirmation)

**Confirmed by the owner on 2026-10-26: P1–P20 exactly as recommended (C19).** The "Alternative" column is kept as
a record only.

Each row states the recommendation this plan assumes. Steps that depend on a pending decision cite its ID. If the
owner chooses differently, those steps change before execution.

| ID | Decision | Recommendation (assumed below) | Alternative |
|---|---|---|---|
| **P1** | Line endings. Initial's `schema.dql` is CRLF in this Windows working tree (`core.autocrlf=true`, no **root** `.gitattributes`; only `parity/.gitattributes` exists, `* text eol=lf`, which is why the fixture files are LF); the committed blob is LF and equals Python's. A byte comparison fails on this checkout today. Stage 0 also found 79 of Initial's 82 `.go` files CRLF in the working tree; this plan does not renormalise them (see V8) | Add a root `.gitattributes` with `* text=auto eol=lf`, as Python has | Pin only `schema.dql` and `*.sh` |
| **P2** | How the schema test covers the `links` edge, which `streamLinks` emits from database rows | Port the stub `database/sql` driver from `parity_stubdb_test.go` into `internal/etl/initial`; no database needed | Require the fixture's PostgreSQL for every `go test ./...` |
| **P3** | Location of Python's schema for the test | Fixed sibling path `<Initial root>/../ETL-Dgraph-Incremental/schema.dql`, no override | Also allow an environment-variable override (still fails when missing) |
| **P4** | Architecture the test suite runs on | Unit tests as `linux/amd64` in the container (pure Go). arm64 proven by the cross-compiled binary plus one emulated `linux/arm64` smoke run of that binary | The whole test suite under `linux/arm64` emulation (slower). C15 says "targeting linux/arm64"; confirm which reading applies |
| **P5** | Container mounts, required by C7's fail-if-missing rule (both scripts mount only Initial today) | `parity/go.sh` and Python's `scripts/demo_guards_offline.sh` mount both repositories side by side as `/work/<repo>` | None that keeps C7: without it the schema test fails in both scripts |
| **P6** | Build mechanism | New root `build.sh` that always uses `golang:1.25.3-bookworm` and cross-compiles with `CGO_ENABLED=0 GOOS=linux GOARCH=arm64 -trimpath -ldflags="-s -w"` | Extend `parity/go.sh` with environment pass-through |
| **P7** | Contents of `out/` | Binary plus `out/config/app.yaml` (the binary reads `config/app.yaml` relative to its working directory) | Binary only |
| **P8** | `go.mod` / `go.sum` | Leave untouched during execution (unused requirements do not break a build); run `go mod tidy` and `gofmt` in Phase 4 as their own commit | Hand-edit during execution |
| **P9** | Merge gate | Branches stay unmerged until every Phase 4 check passes (§7) | Merge after review only |
| **P10** | Historical label for `BASE_KNOWLEDGE.md` | One whole-file banner ("historical snapshot at `b487294`"), body unchanged | Per-section labels |
| **P11** | Historical label for `setup/` | New `setup/README.md` | Mention only in `CLAUDE.md` |
| **P12** | Python `tests/test_retry.py` reads Initial's `pkg/dgraph/dgraph.go` from the working tree and would silently skip after removal | Read the file from Initial's history at `b487294`, with `git`, which the test containers now have (C17). Skip only when the Initial repository itself is absent; with Initial present, a failing `git show` **fails** the test, so a missing git or `safe.directory` can never turn into a skip | Accept the skip |
| **P13** | Drift check before a bulk reload (P7-11 in the live plan) | Add a step to Python's `INV7_BULK_LOAD.md` (in `deploy/staging/`): run Initial's test suite before any reload | Rely on the test alone |
| **P14** | `REFACTOR_PLAN.md` citations inside the frozen fixture files (`compose.yaml:1`, `sql/01_schema.sql:1`, `sql/02_seed.sql:2`, and the string at `gen/main.go:426`) cannot change without breaking both checksum locks | Leave them; they refer to the Dgraph copy by convention | None |
| **P15** | `.env.example` | Remove `DGRAPH_*`, set `APP_MODE=initial`, add `INITIAL_WORKER_COUNT` and `INITIAL_CHUNK_SIZE` | Only remove `DGRAPH_*` and set `APP_MODE=initial` |
| **P16** | Commit granularity and pre-commit check for Phase 0 | Initial 2 commits (Dockerfiles; the `REFACTOR_PLAN.md` sync, C16), Dgraph 1, Python 5 (the first adds the pre-check runner; the last adds `git` to the test containers, C17). Run the Python repository's stdlib-only `tests/guards_precheck.py` runner (the `tests/guards.py` checks over an explicit file list) with local Python first | One commit per repository |
| **P17** | Branch names | Initial `chore/remove-incremental`, Python `docs/initial-incremental-removal`, Dgraph `docs/record-incremental-removal` | Owner's choice |
| **P18** | How decisions are recorded in the live plan | As the next owner-decision IDs after M13 (M14 onward), plus status updates on P7-7 … P7-12 | Owner's choice |
| **P19** | Pushing | Nothing is pushed; pushing the Dgraph repository to its remote stays an owner action | — |
| **P20** | This file's lifecycle. It is untracked now and must not be swept into the Phase 0 commits | Commit it alone on Initial's `main` right after Phase 0 (`docs: add the incremental-removal plan`); delete it in a follow-up commit after the merge, once the live plan records the outcome | Keep it untracked; or commit it on the cleanup branch |

---

## 3. Entry criteria

Execution may begin only when all of these hold:

1. Every pending decision in §2.2 is confirmed or replaced.
2. The owner explicitly authorises execution.
3. The working trees match the state recorded in §4.1. If they do not, stop and re-confirm.
4. C16–C18 are recorded (they are, in this file) and the Stage 1 blocker found by Stage 0 is resolved: §4.1 now lists
   every pending entry Stage 0 saw, and a fresh `git status --short` in each repository equals §4.1 exactly. Stage 9
   additionally requires V19.

---

## 4. Phase 0: commit existing pending work on `main`

### 4.1 Pending state at the time of writing

| Repository | Pending |
|---|---|
| Initial | modified `deployment/dockerfiles/Dockerfile-{dev,prd-1,prod,stg}` (`GOARCH` amd64 → arm64; binary name `dgrapg` → `dgraph`); modified `REFACTOR_PLAN.md` (+658/−281, **byte-identical to Dgraph's pending copy**, sha256 `cd05e357…e43f`; C16); `REFACTOR_ASSESSMENT.md` listed as modified **without any content change** (working copy = `HEAD` = Dgraph's `HEAD` copy, sha256 `750fbf39…3f8a`; the index holds a stale stat entry, size 29929 vs 29569 on disk; C16); untracked `INCREMENTAL_REMOVAL_PLAN.md` (this file). Also present, not pending: the local branch `p1-parity-harness` (at `79a38f0`, kept) |
| Dgraph | modified `REFACTOR_PLAN.md` (P5 conditional acceptance, M10–M13, INV-7 partly resolved; sha256 `cd05e357…e43f`, `HEAD` blob `91a32fe4…abdf`); `main` is 5 commits ahead of its remote |
| Python | modified `deploy/staging/DEPLOYMENT_LOG.md`, `docs/ECS_TASK.md`, `docs/HARNESS.md`; untracked `INV7_BULK_LOAD.md` and `inv7_bulk_rehearsal.sh` (both in `deploy/staging/`), `docs/diagrams/ecs-execution-flow.svg`, `docs/diagrams/ecs-execution-flow.png`; untracked `tests/guards_precheck.py` and `tests/test_guards_precheck.py` (the pre-check runner and its test, added after Stage 0); **added after Stage 0 for C17:** modified `scripts/test.sh`, `scripts/demo_guards_offline.sh` and `README.md` (one line, in the file list), untracked `scripts/test.Dockerfile` and `scripts/test_image.sh` |

### 4.2 Steps

1. Pre-check (P16, no Docker): from Python's repository run
   `python -I tests/guards_precheck.py <every file listed below, as an explicit path>`. The runner applies the dump,
   credential-file and entropy checks of `tests/guards.py` (stdlib only) to exactly those files; it reports the check,
   file and line, never the matching text. Exit 0 is clean, 1 is a hit, 2 is a bad list. Any hit stops Phase 0. The
   list includes the runner and its test, covers all three repositories, and includes **every file of the table
   below** (among them Initial's `REFACTOR_PLAN.md` and the Python `scripts/test.Dockerfile` / `scripts/test_image.sh`),
   plus Initial's `REFACTOR_ASSESSMENT.md`, which has no commit but is checked all the same (C16: no file that
   Commit 8 later deletes may skip the check), plus `INCREMENTAL_REMOVAL_PLAN.md`.
2. Stage **by explicit path only**. Never `git add -A` or `git add .`.
3. Commits, each ending with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (C20):

| Repository | Commit message | Files |
|---|---|---|
| Initial | `build: target arm64 and fix the binary name in the per-env Dockerfiles` | the four `deployment/dockerfiles/Dockerfile-*` |
| Dgraph | `docs: record P5 acceptance, decisions M10–M13 and INV-7 in the plan` (**first**: Initial's sync commit below depends on it) | `REFACTOR_PLAN.md` |
| Initial | `docs: sync the REFACTOR_PLAN.md copy with the Dgraph copy` (C16; only after the Dgraph commit above and only if the hash check in step 3a passes) | `REFACTOR_PLAN.md` only. **No commit for `REFACTOR_ASSESSMENT.md`**: it has no content change |
| Python | `test: add a pre-check runner for the data guards` | `tests/guards_precheck.py`, `tests/test_guards_precheck.py` |
| Python | `docs: record decisions M10 and M13 in the deployment docs` | `deploy/staging/DEPLOYMENT_LOG.md`, `docs/ECS_TASK.md`, `docs/HARNESS.md` |
| Python | `docs: add the INV-7 bulk-load record and its local rehearsal` | `INV7_BULK_LOAD.md`, `inv7_bulk_rehearsal.sh` (both in `deploy/staging/`) |
| Python | `docs: add the ECS execution-flow diagram` | `docs/diagrams/ecs-execution-flow.svg`, `docs/diagrams/ecs-execution-flow.png` (C14) |
| Python | `test: build the test containers with git for the historical Go check` (C17) | `scripts/test.Dockerfile`, `scripts/test_image.sh`, `scripts/test.sh`, `scripts/demo_guards_offline.sh`, `README.md` (the one file-list line) |

3a. Before Initial's second commit, recompute at execution time (do not trust the values recorded here):
    `sha256sum` of Initial's working `REFACTOR_PLAN.md` must equal `git -C ../Dgraph show HEAD:REFACTOR_PLAN.md | sha256sum`
    (the Dgraph commit from step 3, expected `cd05e357…e43f`). Equal: commit it. **Different: stop**; the copies have
    diverged and the owner decides, because nothing may be overwritten or discarded (C13, C16).
4. P20: commit this file alone on Initial's `main`.
5. Exit check, per repository: `git status --short` is empty, with **one** permitted exception, in Initial only: a
   ` M REFACTOR_ASSESSMENT.md` line that is a stat-only artefact (C16). It is accepted only when **all** of these
   hold, run in this order:
   1. `git update-index --refresh` has been run first (it rewrites only index stat data, not content or the working
      tree; if the line disappears, the exception is not needed);
   2. `git diff --exit-code -- REFACTOR_ASSESSMENT.md` exits 0;
   3. `sha256sum REFACTOR_ASSESSMENT.md` = `git show HEAD:REFACTOR_ASSESSMENT.md | sha256sum` = Dgraph's
      `git show HEAD:REFACTOR_ASSESSMENT.md | sha256sum` (expected `750fbf39…3f8a`).
   Any other line in any repository, or any failure of (2) or (3), stops Phase 0. Never clear the entry with
   `git checkout`, `git restore`, `git stash` or `git reset`, which could discard content.

---

## 5. Branch and commit strategy

- One branch per repository, each created from its `main` after Phase 0 (names per P17).
- Commits are small and single-purpose, in the order below, so each can be reviewed and reverted alone.
- Commit messages follow each repository's existing convention (`feat:`, `fix:`, `test:`, `build:`, `docs:`,
  `chore:`, `refactor:`) and end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>` (C20).
- Commits made in Phases 1–3 are **unverified** until Phase 4 (C15). Nothing merges before Phase 4 passes (P9).
- History is never rewritten on `main`: the golden provenance commit `b487294` must stay resolvable in Initial.
- No pushes (P19).

---

## 6. Implementation steps

### 6.1 Phase 1: Initial, branch `chore/remove-incremental`

**Commit 1 — `chore: pin LF line endings`** (P1)
- Add root `.gitattributes`: `* text=auto eol=lf`.
- Refresh the working copy of `schema.dql` (it has no pending changes), so it is LF on disk. `cmp` against
  Python's `schema.dql` must then succeed.

**Commit 2 — `refactor: remove the incremental mode and the Dgraph client`**
- Delete `internal/etl/incremental/` (10 source files, 8 `parity_*_test.go` files) and `pkg/dgraph/`.
- `main.go`: remove the `incremental` and `dgraph` imports, the Dgraph connection block, the
  `cfg.Incremental` mapping, `toDgraphConfig` and `toIncrementalConfig`.
- `internal/etl/runner.go`: remove `RunnerConfig.Incremental`, `Dependencies.Graph` and the incremental case.
- `internal/app/config.go`: remove `DgraphConfig`, `IncrementalConfig` and its three sub-structs, the
  `Config.Dgraph` / `Config.Incremental` fields and their environment overrides.
- `config/app.yaml`: remove the `dgraph:` and `incremental:` sections.
- `.env.example`: remove `DGRAPH_*` (P15).
- `go.mod` / `go.sum`: unchanged (P8).

**Commit 3 — `feat: accept only APP_MODE=initial`** (C4)
- `internal/etl/mode.go`: `ParseMode(value string) (Mode, error)`. Trim and lower-case; `initial` → `ModeInitial`.
  **Delete the `ModeIncremental` constant** (V3 allows no `ModeIncremental` hit): the `incremental` error case
  compares the lower-cased value with the string literal `"incremental"`.
  Distinct errors:
  - empty → `APP_MODE is empty; set APP_MODE=initial`
  - `incremental` → `APP_MODE=incremental is no longer supported here: the incremental ETL lives in ETL-Dgraph-Incremental`
  - anything else → `unsupported APP_MODE "<value>": only "initial" is supported`
- `main.go`: parse the mode immediately after the configuration is loaded and before
  `database.NewDatabaseConnection`; return the error unchanged.
- `internal/etl/runner.go`: `Run` rejects any mode other than `ModeInitial`.
- `internal/app/config.go`: if the yaml has no `app:` section, create an empty `AppConfig` before the environment
  overrides, so a missing section produces the mode error instead of a nil-pointer panic.
- `config/app.yaml`: comment states that only `initial` is accepted. `.env.example`: `APP_MODE=initial`;
  add `INITIAL_WORKER_COUNT=` and `INITIAL_CHUNK_SIZE=` (P15).
- New `internal/etl/mode_test.go`: table test. Accept `initial`, ` Initial `, `INITIAL`. Reject ``, `   `,
  `incremental`, `Incremental`, `initail`, `$(APP_MODE)`.

**Commit 4 — `test: drop the golden corpus and pin the frozen fixture`** (C5, C6)
- Delete `parity/golden/` (28 files).
- `parity/guards/guards_test.go`:
  - remove the `goldens != 28` check; keep the golden scan loop and every other guard;
  - add a fixture checksum guard pinning these SHA-256 values (identical to Python's `tests/fixture.lock`):

    | File (under `parity/fixture/`) | SHA-256 |
    |---|---|
    | `sql/01_schema.sql` | `ecaab79678dab1e34e9bccd218d5d9e111cf2659be6ea66968f576c631ecd5cd` |
    | `sql/02_seed.sql` | `62df94ff0139b71e44dc7bc83bb93597e251fbc0b1e1bd059376a6d5ec16fc03` |
    | `graph_seed.json` | `bd7729cc5f176d9e3902b71932be2bfca7357b6e24049516cbbd6bb3edd77799` |
    | `scenarios.json` | `1deeb55a4e0fbbc08e2c87b4bcde878bbc86cf2a60a7f034f47464fa1f0bb9b6` |
    | `compose.yaml` | `4a47fe237d949974b2d43f9a6ececbff9ba13a42582b5f4c2de831a26a9deb83` |
    | `rebuild.sh` | `4625c84d6ea7edeaaa18691e1205a76a173049538b7daefb6a8d92f0d49000e1` |

  - add `TestNegative…` for the checksum guard (a planted changed copy must fail), matching the file's convention
    that every guard has a negative test;
  - update the header comment: (c) reads "nothing in the fixture or goldens looks real" and mentions no count (the
    28 lives only in the code, `guards_test.go:515-517`), so drop "or goldens"; add (e) the fixture checksum.
- Frozen fixture files are not edited (P14).

**Commit 5 — `test: validate emitted predicates against the canonical schema`** (C7, P2, P3)
- New `internal/etl/initial/stubdb_test.go`: the stub `database/sql` driver, ported from
  `parity_stubdb_test.go` (connector, conn, rows; no `Exec`/`BeginTx` support).
- New `internal/etl/initial/schema_test.go`:
  - locate the repository root from the test file; read `../ETL-Dgraph-Incremental/schema.dql`; if absent,
    `t.Fatalf` with the path and how to fix it (place the Python repository beside Initial). Never `t.Skip`;
  - **mirror test**: Initial's `schema.dql` is byte-identical to Python's;
  - **events**: build an `EventRow` whose every field is populated by reflection (`Valid: true`, non-empty value),
    pass it through `newAggregator(t.TempDir(), …)` / `emitEvent` / `Close`, decompress every `events*.rdf.gz`
    chunk, and parse each triple's predicate and object type (`"…"` → `string`, `^^<…#int>` → `int`,
    `^^<…#float>` → `float`);
  - **links**: run `streamLinks` over the stub driver with one fully populated link row; expect a `<links>` triple
    whose object is a blank node → `[uid]`;
  - **assertions, both directions**: emitted predicates (excluding `dgraph.type`) == schema scalar declarations ==
    fields of `type Event`; every type matches; the `dgraph.type` object equals the schema's type name `Event`.
- Expected result today: 43 scalar predicates plus `links`; `process_date` `int`, `amount` `float`, all others
  `string`.

**Commit 6 — `build: build a standalone linux/arm64 binary and drop the ECS deployment`** (C2, C3, P5, P6, P7)
- Delete `deployment/` and the root `Dockerfile`.
- New root `build.sh` (LF via P1): `docker run --rm` with `golang:1.25.3-bookworm` (host-native image), the
  repository mounted, the shared module and build caches (`parity-gomod`, `parity-gocache`), and
  `CGO_ENABLED=0 GOOS=linux GOARCH=arm64`; runs
  `go build -trimpath -ldflags="-s -w" -o out/traceability-dgraph-etl .` and copies `config/app.yaml` to
  `out/config/app.yaml`. Always uses the container, even when Go is installed locally.
- `.gitignore`: add `out/`.
- `parity/go.sh`: mount the parent folder's `ETL-Dgraph-Initial` (read-write) and `ETL-Dgraph-Incremental`
  (read-only) as `/work/<repo>`, working directory `/work/ETL-Dgraph-Initial`; stop with a clear error if the
  Python repository is missing; replace the `./internal/etl/incremental … -update` example in the header.

**Commit 7 — `chore: move the Dgraph host scripts to setup/`** (C9, P11)
- `git mv scripts setup`; script contents unchanged.
- New `setup/README.md`: historical, unmaintained; written for x86 `c5d` hosts, three replicated zeros and the
  amd64 `latest` binary; the live Staging hosts are arm64 `c6gd`, one zero and one alpha, on v25.3.1 (INV-7).

**Commit 8 — `docs: describe the Initial-only repository`** (C10, C11, P10)
- Rewrite `CLAUDE.md` for Initial only: build through `build.sh`, test through `parity/go.sh`, the sibling
  requirement, configuration flow without CI or incremental steps, strict `APP_MODE`, the byte-identical schema
  rule (Python is canonical; the bulk load uses Python's copy), the frozen fixture and its guard, `setup/` as
  historical, no deployment.
- Rewrite `parity/README.md`: the frozen fixture for Python, the checksum guard, the generator kept as a
  reproducibility check; the golden corpus as history (Python `tests/golden/`, commit `b487294`).
- `BASE_KNOWLEDGE.md`: whole-file historical banner, body unchanged.
- Delete `REFACTOR_PLAN.md` and `REFACTOR_ASSESSMENT.md` (C10, C16), **only after V18 passes** in the same sitting.
  Use plain `git rm` with no `-f`: git refuses to remove a file with uncommitted changes, which is the last safety
  net. Both files stay recoverable from Initial's history and from Dgraph's `main`.
- Repoint comments to the Dgraph copy: `parity/guards/guards_test.go:1`, `parity/fixture/gen/main.go:1`
  (not line 426; P14).
- `command.txt`: unchanged.

### 6.2 Phase 2: Python, branch `docs/initial-incremental-removal`

- `README.md:223–224`: Initial keeps a byte-identical copy of `schema.dql`, enforced by Initial's schema test;
  this repository owns the schema; the bulk-load runbook uses this copy.
- `README.md:643–658`: the fixture is frozen; the Go goldens can no longer be regenerated; Initial pins the same
  six hashes.
- `tests/golden/SOURCE.md:15–16`, `tests/fixture.lock:3–4` (comment lines only), `scripts/fixture.sh:34` (error
  message): replace "regenerate the goldens there" with the frozen-fixture rule.
- `scripts/demo_guards_offline.sh`: mount both repositories as `/work/<repo>` (this repository read-only) for
  Initial's `go test` (P5). This builds on the Phase 0 edit that gave the pytest container `git` (C17); do not undo
  `ensure_test_image`, `TEST_GIT_ENV` or `GIT_PROOF` while editing.
- `tests/test_retry.py:37–46`: read `pkg/dgraph/dgraph.go` from Initial's history at `b487294` with
  `git -C <Initial> show b487294:pkg/dgraph/dgraph.go` (P12). `git` and `safe.directory` come from the test
  containers (C17). Keep the existing skip only when the Initial repository itself is absent; with Initial present,
  any failure of `git show` fails the test.
- `INV7_BULK_LOAD.md:17` (in `deploy/staging/`): `scripts/` → `setup/`; add the pre-reload step "run Initial's test
  suite" (P13).
- Do not edit `tests/golden/FORMAT.md` or any golden: they are hashed by `MANIFEST.sha256`.

### 6.3 Phase 3: Dgraph, branch `docs/record-incremental-removal`

In `REFACTOR_PLAN.md` (P18):
- Owner decisions C1–C18 as M14 onward (C16–C18 included).
- P7-7 (remove `internal/etl/incremental`, `pkg/dgraph`), P7-8 (simplify dispatch), P7-12 (feature-frozen
  repository): done on the branch.
- P7-9 **reversed**: the copy is kept, byte-identical and test-enforced; supersedes assessment item I1.
- P7-10: the runbook uses Python's `schema.dql`. P7-11: covered by Initial's schema test, run before any reload.
- Open question F3 answered: Initial runs ad hoc as a local `linux/arm64` binary, not on ECS.
- Line 274 ("which copy is canonical"): the Dgraph copies; Initial's copies are deleted.
- INV-7 H1 (how the zero and alpha are run on the hosts; "the repository's scripts are stale"): it is row H1 of the
  INV-7 record (`INV7_BULK_LOAD.md:99`) and is summarised in the "Open before P6-0b: H1–H6" bullet of the INV-7
  section (`REFACTOR_PLAN.md:1069`). It is **not** the unrelated "H1 Repository strategy" row of the owner-decision
  table (`REFACTOR_PLAN.md:1359`). Record there that the stale host scripts now live in Initial's `setup/`.
- Rollback note: Initial no longer contains Go Incremental; the Go image and `td-go-rollback.json` remain the
  rollback artefact.
- Do not change §8.5 (the key table and the task-role IAM policy): Python tests parse it.

---

## 7. Validation requirements

### 7.1 Before Docker (during execution)

| # | Check | Expected |
|---|---|---|
| V1 | `cmp schema.dql ../ETL-Dgraph-Incremental/schema.dql` after Commit 1 | identical |
| V2 | `sha256sum -c` of the six fixture files against Python's lock | all `OK` |
| V3 | Search Initial (excluding `.git/`) for `internal/etl/incremental`, `pkg/dgraph`, `dgraph.Adapter`, `DGRAPH_`, `INCREMENTAL_`, `ModeIncremental`, `scripts/setup_dgraph` | only historical mentions in `BASE_KNOWLEDGE.md`, `command.txt`, the frozen fixture files and `setup/README.md` |
| V4 | `python -I tests/guards_precheck.py` over every changed or added file in the three repositories (explicit paths) | exit 0, no hits |
| V5 | `python -I -m py_compile tests/test_retry.py` (Python repository) | compiles |
| V6 | `git status --short` after each phase | only the intended files changed |
| V18 | Stale-copy preservation, run **before Commit 8 deletes** Initial's `REFACTOR_PLAN.md` and `REFACTOR_ASSESSMENT.md` (C16): for each file, `git -C ETL-Dgraph-Initial show HEAD:<file> \| sha256sum` equals `git -C Dgraph show main:<file> \| sha256sum`; and `git -C ETL-Dgraph-Initial status --short -- <file>` is empty (the §4.2 step 5 exception must not be needed any more). Dgraph's `main` still holds the Phase 0 blob, because Phase 3 edits only its branch | both hashes equal (`REFACTOR_PLAN.md` expected `cd05e357…e43f`, `REFACTOR_ASSESSMENT.md` expected `750fbf39…3f8a`, recomputed at execution time); otherwise **stop, do not delete** |

### 7.2 Phase 4: after Docker is started (C15)

| # | Check | Expected |
|---|---|---|
| V7 | Docker Desktop running with `linux/arm64` emulation; `parity/fixture/rebuild.sh` | fixture database ready |
| V8 | `parity/go.sh mod tidy`; `gofmt -l .` | `go.mod` / `go.sum` drop `dgo/v250`, `grpc` and their unused indirect modules; `gofmt -l` lists no file introduced by this branch: it may list only the five files it already lists on `main`, which are pre-existing, out of scope (§1) and left unchanged (`internal/etl/common/chunked_rdf_writer.go`, `parity/guards/guards_test.go`, `pkg/errors/validation.go`, `pkg/util/random_generator_test.go`, `pkg/util/replacer_test.go`; owner decision, 2026-10-26). Committed as `build: tidy modules after removing the Dgraph client` (P8). **Open (Stage 0 audit):** `gofmt -l` also lists CRLF files and `parity/go.sh` has no way to run `gofmt`; both need an owner decision before this check is meaningful |
| V9 | `parity/go.sh vet ./...`; the same with `GOOS=linux GOARCH=arm64` in the build container | clean |
| V10 | `parity/go.sh test ./...` | all pass: mode, schema, mirror, guards (with the checksum guard and its negative test), generator |
| V11 | Negative: move `../ETL-Dgraph-Incremental` aside and rerun the schema test | **fails** with the clear message (never skips) |
| V12 | Negative: add a predicate to a scratch copy of the schema used by the test | fails, naming the predicate |
| V13 | `bash build.sh`; inspect `out/traceability-dgraph-etl` | `ELF 64-bit LSB executable, ARM aarch64, statically linked`; `out/config/app.yaml` present |
| V14 | Smoke run under `--platform linux/arm64` with `out/` mounted, database host unreachable: `APP_MODE` unset-in-yaml-and-empty, `bogus`, `incremental` | exit ≠ 0; the matching mode error; no connection attempt logged |
| V15 | Smoke run under `--platform linux/arm64`, `APP_MODE=" Initial "`, against the fixture | exit 0; 14 event subjects in `events*.rdf.gz`; 8 `<links>` edges in `links*.rdf.gz` (as the INV-7 rehearsal) |
| V16 | Python repository: `scripts/test.sh`, `scripts/demo_guards_offline.sh` | pass (the offline demo runs Initial's `go test` with both repositories mounted) |
| V17 | `git -C ETL-Dgraph-Initial cat-file -t b487294` | `commit` |
| V19 | Git inside the Python test containers (C17, Stage 9's prerequisite): `scripts/test.sh` and `scripts/demo_guards_offline.sh` build `etl-incr-test:<hash>` once; inside that image, with Initial mounted read-only at `/work/ETL-Dgraph-Initial`, `git --version` works and `git -C /work/ETL-Dgraph-Initial show b487294:pkg/dgraph/dgraph.go` prints `package dgraph` (the demo prints this as its `GIT_PROOF` line and fails otherwise). Negative: the same `git show` **without** `GIT_CONFIG_*` fails with "dubious ownership" (shows the `safe.directory` setting is what makes it work) | the proof line appears and both scripts pass; `tests/test_retry.py::test_the_lists_equal_the_go_source_when_it_is_available` **runs and passes, not skipped** (`pytest -rs` shows no skip for it); the image's `git` version is recorded in the results. If the negative does not fail on this Docker setup (root-owned mounts), record that and keep the setting |

---

## 8. Merge gates

| Gate | Condition |
|---|---|
| **G1** | Every §7 check passes and the results are recorded |
| **G2** | The owner reviews and approves each branch |
| **G3** | Initial and Python branches merge **together**: Initial's schema test needs Python's updated offline demo, and Python's references describe Initial's new state |
| **G4** | The Dgraph branch merges after G3, so the live plan never records an unmerged state as done |
| **G5** | Merge by fast-forward or merge commit only; no rebase or squash of existing `main` history; `b487294` resolves (V17) |
| **G6** | No push without a separate owner instruction (P19) |

---

## 9. Rollback

- Before merge: delete the branch; `main` is untouched apart from the Phase 0 commits, which only record work that
  already existed.
- After merge: revert the merge commits. Phase 0 commits stay.
- Go Incremental remains recoverable from history (`b487294` and earlier) and from the original `Dgraph`
  repository. The deployed rollback for the incremental job is the Go image in ECR with `td-go-rollback.json`,
  unaffected by this work.

---

## 10. Risks

| Risk | Mitigation |
|---|---|
| Phase 1–3 commits are not compiled or tested until Phase 4 | Small single-purpose commits; G1 blocks the merge |
| Initial's tests now require the Python repository beside it (by design, C7) | `parity/go.sh` checks and explains; any future CI must check out both |
| The two `schema.dql` copies stay identical only where the tests run | Mirror test; runbook uses Python's copy (C8) and runs Initial's tests first (P13) |
| A `.env` with a blank `APP_MODE=` now fails | `.env.example` ships `APP_MODE=initial`; the error says what to set |
| A native cgo build would link Debian 12 glibc and could fail on Amazon Linux | `CGO_ENABLED=0` is mandatory in `build.sh` (P6); V13 checks "statically linked" |
| The frozen fixture is now guarded in two repositories | Both pin identical hashes; any change needs both updated together, which the freeze forbids anyway |
| Deleting Initial's stale `REFACTOR_PLAN.md` / `REFACTOR_ASSESSMENT.md` could lose content (C16) | Phase 0 commits the pending `REFACTOR_PLAN.md` change after a recomputed hash match with Dgraph's commit; V18 re-proves both copies before Commit 8; `git rm` without `-f`; both stay in history |
| The test image needs the network once to install `git`, and the Debian `git` version is not pinned (C17) | The base is digest-pinned, the tag follows the Dockerfile's hash, the offline demo builds it in its network-on setup; V19 records the version. A fully hermetic image (git `.deb` pinned by version and hash) is not planned and would be a new decision |
| Docker is inactive (C15), so the container change is unproven until V19 | Static checks only so far (`bash -n`, the tag derivation, the generated `docker run` flags); Stage 9 stays blocked on V19 (C18) |
| `scripts/local_stack.sh` runs Python containers on bare `python:3.12.3-slim` (`pyrun`, `measure`) | Neither runs `test_retry.py`; `local_stack.sh test` calls `scripts/test.sh` and so gets the new image. Left unchanged on purpose |
