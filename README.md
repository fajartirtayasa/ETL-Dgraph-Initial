# ETL-Dgraph-Initial

A Go batch job that exports traceability data from PostgreSQL into RDF files, ready to be bulk-loaded into Dgraph.

## Project Overview

HARA's traceability data (supply-chain **events** and the **links** between them) lives in PostgreSQL and is also
served as a graph from Dgraph. This repository builds that graph's starting point: a one-off **initial export** of
millions of rows, written as compressed RDF N-Quad files that the Dgraph bulk loader ingests.

It is one of three sibling repositories:

| Repository | Role |
|---|---|
| `ETL-Dgraph-Initial` | this repository: the initial (bootstrap) export |
| `ETL-Dgraph-Incremental` | the Python ETL that keeps Dgraph up to date day by day; owns the canonical `schema.dql` |
| `Dgraph` | the refactor plan and assessment documents |

The daily incremental sync used to live here as a second mode. It has moved to `ETL-Dgraph-Incremental`, so this
repository now does the initial export only.

## Main Functionality

1. **Export events.** Reads valid events that take part in at least one valid link, enriched with location, source
   and mill details, and writes them as RDF.
2. **Export links.** Reads the valid links between those events and writes them as RDF edges.
3. **Stamp `sync_time`.** Once every file is complete, sets `sync_time = NOW()` on the exported source rows in
   `events_bc` and `links_bc`. This marks them as already loaded, so the incremental ETL starts from this baseline
   instead of reprocessing everything. It is the job's only write to PostgreSQL and can be turned off with
   `INITIAL_STAMP_SYNC_TIME=false`.

The output goes to `./initial_imports/` (recreated on every run):

- `events.rdf.gz`, `events_1.rdf.gz`, … for event nodes
- `links.rdf.gz`, `links_1.rdf.gz`, … for the edges between events

Files roll over every 1,000,000 records by default. The job **does not connect to Dgraph**: loading the files with
`dgraph bulk` and applying `schema.dql` to the cluster are separate, manual steps.

## Execution Overview

The job is a single binary with one mode, `initial`. A typical run:

1. **Build** the static `linux/arm64` binary (Docker required, no local Go needed):

   ```bash
   bash build.sh
   ```

   This produces `out/traceability-dgraph-etl` and `out/config/app.yaml`.

2. **Configure** the PostgreSQL connection in `config/app.yaml`, or override any value with environment variables
   (or a `.env` file; see `.env.example`). `APP_MODE` must be `initial`.

3. **Run** the binary from a directory that contains `config/` (the repository root, or `out/`):

   ```bash
   cd out && ./traceability-dgraph-etl
   ```

The job logs JSON to stdout and exits with code `1` on any failure. If the export succeeds but the `sync_time` stamp
fails, the RDF files are kept and the error message explains how to finish the stamp by hand.

There is no deployment pipeline: the binary is run on demand on a Linux arm64 host (EC2).

### Main settings

| Variable | Purpose | Default |
|---|---|---|
| `APP_MODE` | Must be `initial`; anything else stops the job at startup | `initial` |
| `DB_CLEAN_*` | Source PostgreSQL connection | from `config/app.yaml` |
| `INITIAL_WORKER_COUNT` | Number of parallel workers building RDF | number of CPUs |
| `INITIAL_CHUNK_SIZE` | Records per output file before rolling over | 1,000,000 |
| `INITIAL_STAMP_SYNC_TIME` | Stamp `sync_time` after the export (`true` / `false`) | `true` |
| `LOGGER_LEVEL` | `debug`, `info`, `warn` or `error` | `debug` (from `config/app.yaml`) |

## Key Components

- **Go 1.25** for a single, statically linked binary.
- **PostgreSQL** as the source, read through a small `sqlx` + `pgx` adapter.
- **RDF N-Quads (gzip)** as the output format, consumed by the **Dgraph bulk loader**.
- **Concurrent pipeline**: one reader streams rows to a pool of workers, which feed a chunked gzip writer.
- **Docker** (`golang:1.25.3-bookworm`) for building and testing, so every machine uses the same toolchain.

## Repository Structure

```
main.go               Entry point: loads config, connects to PostgreSQL, runs the export
config/app.yaml       Default configuration (every value can be overridden by an env var)
.env.example          List of supported environment variables
build.sh              Builds the linux/arm64 binary into out/
schema.dql            Dgraph schema (a copy of the canonical one in ETL-Dgraph-Incremental)
internal/
  app/                Configuration loading
  etl/                Mode checks and the runner
    initial/          The export pipeline (events, links) and the sync_time stamp
    common/           Shared RDF helpers: blank nodes, string interning, chunked gzip writers
pkg/                  Reusable infrastructure (database adapter, logging, utilities)
parity/               Test tooling: Go runner script, frozen synthetic fixture, data and credential guards
setup/                Historical Dgraph host-provisioning scripts (not maintained)
```

Other useful documents:

- `CLAUDE.md`: short guide to commands, configuration rules and invariants to respect when changing the code.
- `BASE_KNOWLEDGE.md`: detailed reference for the queries, the stamp and the tests.
- `command.txt`: the original requirements brief, including field mappings.
- `parity/README.md`: the test fixture and guards.

## Running the Tests

```bash
parity/go.sh test ./...
```

The tests expect `ETL-Dgraph-Incremental` to be checked out next to this repository, because they compare
`schema.dql` with its canonical copy there.
