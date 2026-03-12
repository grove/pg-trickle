# pg_trickle Getting Started — dbt Example Project

A self-contained dbt project that mirrors every step of the
[Getting Started guide](../../docs/GETTING_STARTED.md) using dbt models and
seeds. It demonstrates the `stream_table` materialization, schedule
inheritance, and stream-table-to-stream-table chaining on a small
department/employee org-chart dataset.

## What this project builds

```
seeds/                     dbt seeds (loaded as plain tables)
  departments.csv          7 departments in a self-referencing tree
  employees.csv            7 employees assigned to leaf departments

models/staging/            pass-through views (not stream tables)
  stg_departments          thin SELECT over the departments seed
  stg_employees            thin SELECT over the employees seed

models/orgchart/           incremental stream tables maintained by pg_trickle
  department_tree          recursive CTE — flattens the hierarchy (CALCULATED)
  department_stats         per-department headcount + salary (CALCULATED)
  department_report        top-level division rollup, schedule = 1 minute
```

The two CALCULATED tables (`department_tree`, `department_stats`) inherit their
refresh cadence from `department_report`'s 1-minute schedule. pg_trickle's
scheduler propagates the schedule automatically through the DAG.

## Prerequisites

- Docker (to run the pg_trickle E2E image)
- Python 3.12 or 3.13 with pip
- dbt 1.9+ (`pip install "dbt-postgres~=1.10.0"`)

No local PostgreSQL installation is required — the run script starts a
containerised pg_trickle instance automatically.

## Quick start — automated (recommended)

```bash
# From the repo root:
./examples/dbt_getting_started/scripts/run_example.sh
```

This script:
1. Builds the pg_trickle E2E Docker image (or skips with `--skip-build`)
2. Starts a PostgreSQL container on port **15433**
3. Runs `dbt deps → dbt seed → dbt run`
4. Waits for all three stream tables to be fully populated
5. Runs `dbt test` (generic + custom tests)
6. Cleans up the container on exit

```bash
# Keep the container running after the script finishes (useful for exploration):
./examples/dbt_getting_started/scripts/run_example.sh --keep-container

# Skip rebuilding the Docker image (faster if already built):
./examples/dbt_getting_started/scripts/run_example.sh --skip-build
```

## Quick start — manual (against an existing pg_trickle instance)

```bash
cd examples/dbt_getting_started

# Install the pg_trickle dbt package
dbt deps

# Load seed data
dbt seed

# Create and populate stream tables
dbt run

# Verify results
dbt test
```

Connection defaults: `localhost:5432`, database `postgres`, user/password
`postgres`. Override via environment variables: `PGHOST`, `PGPORT`,
`PGDATABASE`, `PGUSER`, `PGPASSWORD`.

## justfile targets

```bash
just test-dbt-getting-started        # full run (builds Docker image)
just test-dbt-getting-started-fast   # skip image rebuild
```

## Project structure

```
examples/dbt_getting_started/
├── dbt_project.yml       project metadata, seed post-hooks, model defaults
├── profiles.yml          connection config (env-var driven)
├── packages.yml          installs dbt-pgtrickle from GitHub
├── schema.yml            column docs and generic tests
├── seeds/
│   ├── departments.csv
│   └── employees.csv
├── models/
│   ├── staging/
│   │   ├── stg_departments.sql
│   │   └── stg_employees.sql
│   └── orgchart/
│       ├── department_tree.sql    RECURSIVE CTE, DIFFERENTIAL, CALCULATED
│       ├── department_stats.sql   LEFT JOIN + GROUP BY, CALCULATED
│       └── department_report.sql  division rollup, schedule = 1m
├── tests/
│   ├── assert_tree_paths_correct.sql
│   ├── assert_stats_headcount_matches.sql
│   ├── assert_report_payroll_matches.sql
│   └── assert_no_stream_table_errors.sql
└── scripts/
    ├── run_example.sh          Docker + dbt orchestration
    └── wait_for_populated.sh   polls pgtrickle.pgt_stream_tables
```

## Relationship to the Getting Started guide

Each file in this project corresponds to a section in
[docs/GETTING_STARTED.md](../../docs/GETTING_STARTED.md):

| Guide step | dbt equivalent |
|------------|----------------|
| Create `departments` table | `seeds/departments.csv` + seed post-hook |
| Create `employees` table | `seeds/employees.csv` + seed post-hook |
| Create `department_tree` stream table | `models/orgchart/department_tree.sql` |
| Create `department_stats` stream table | `models/orgchart/department_stats.sql` |
| Create `department_report` stream table | `models/orgchart/department_report.sql` |
| Verify results with SQL | `dbt test` (custom test files) |

## Using a local package checkout

During development, you can point `packages.yml` at your local `dbt-pgtrickle`
checkout instead of fetching from GitHub:

```yaml
# packages.yml (temporary, do not commit)
packages:
  - local: ../../dbt-pgtrickle
```

Then run `dbt deps` again to install from the local path.
