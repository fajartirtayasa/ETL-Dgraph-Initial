# setup/ — historical Dgraph host scripts (unmaintained)

**These scripts are historical. They are not maintained, not part of the ETL, and not run by anything in this
repository.** They were moved here unchanged from `scripts/` (commit "chore: move the Dgraph host scripts to
setup/"); their history is preserved.

| Script | What it does |
|---|---|
| `setup_dgraph_zero.sh` | tunes an Ubuntu / Amazon Linux host (file descriptors, sysctl), installs Dgraph and starts **Zero** under systemd |
| `setup_dgraph_alpha.sh <ZERO_PRIVATE_IP>` | the same for an **Alpha** |
| `swap.sh` | creates a 4 GB swap file |

## What they describe, and why that is out of date

They were written for an **earlier cluster**:

- **x86 hosts** (`c5d.large` for Zero, `c5d.4xlarge` for Alpha, per the sysctl comments);
- **three replicated Zeros** (`dgraph zero --replicas=3`; the Alpha script points at three Zero addresses that are
  hard-coded in the script);
- the **amd64 `latest` Dgraph binary** (`releases/latest/download/dgraph-linux-amd64.tar.gz`), which is not a pinned
  version.

The **live Staging cluster** looks different (verified read-only, 2026-10-20, INV-7 in the Dgraph repository's
`REFACTOR_PLAN.md` and `INV7_BULK_LOAD.md` in ETL-Dgraph-Incremental's `deploy/staging/`):

- **arm64 `c6gd`** hosts;
- **one Zero and one Alpha** (one group, no replication);
- Dgraph **v25.3.1**.

So do not run these scripts against Staging or Production as they stand. How the live Zero and Alpha processes are
actually started on the hosts (systemd, docker or a plain binary, and the `p`, `w` and `zw` paths) is an open item
(INV-7 H1) in the Dgraph repository's plan.

## Where the current guidance lives

- The bulk-load runbook is `INV7_BULK_LOAD.md` in ETL-Dgraph-Incremental (`deploy/staging/`); it uses that
  repository's `schema.dql`, the canonical copy.
- The plan and decisions are in the Dgraph repository's `REFACTOR_PLAN.md`.
