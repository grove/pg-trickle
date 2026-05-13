# `pg_aqueduct` — Declarative Schema Evolution & Migration for Stream-Table DAGs

**Status:** Discussion / proposal — no implementation work committed
**Date:** 12 May 2026
**Author:** GitHub Copilot, on behalf of the `pg_trickle` maintainers
**Related:**
  - [PLAN_RELAY_STANDALONE.md](PLAN_RELAY_STANDALONE.md) — the precedent for
    extracting a tightly-scoped companion repository (`pg_tide`).
  - [REPORT_FUTURE_DIRECTIONS.md](REPORT_FUTURE_DIRECTIONS.md) §§ 1, 4, 7 —
    "default IVM substrate", "self-tuning", and the surrounding ecosystem.
  - [dbt-pgtrickle/README.md](../dbt-pgtrickle/README.md) — the build-time
    integration that `pg_aqueduct` is **not** a replacement for.

---

## 0. TL;DR

> **Yes — `pg_aqueduct` is a real and worthwhile project, and it does not
> belong inside either dbt or `pg_trickle`.** It is the missing
> *state-aware* migration tool for a streaming, incrementally-maintained
> DAG of materialized views. dbt orchestrates *builds* (stateless,
> declarative, recompute on every run); `pg_trickle` orchestrates
> *transitions* (the differential delta of one moment to the next).
> Neither owns the third axis: the *evolution of the DAG itself over
> time* — adding columns, changing aggregations, splitting a node,
> renaming a source — without losing in-flight differential state and
> without taking the pipeline offline.
>
> The recommended shape is a **standalone Rust CLI plus a thin
> companion Postgres extension** (`pg_aqueduct`), shipped from a new
> repository `trickle-labs/pg-aqueduct`. It reads a versioned directory
> of declarative `*.sql` and `aqueduct.toml` files, computes a *plan*
> (diff between desired and actual DAG state), and executes the plan
> against a target database in topologically-correct order, preserving
> materialized state wherever possible. It is to a stream-table DAG
> what **Atlas** is to a relational schema and what **Terraform** is to
> infrastructure.
>
> Estimated effort to a usable v0.1: **~6 weeks** for one engineer.
> v1.0 (online schema evolution, blue/green deployments, drift
> detection, CI integrations) is a **3–6 month** project.

---

## 1. The Problem: Why a Stream-Table DAG Needs its Own Migration Tool

`pg_trickle` users today operate three logical layers:

1. **Base tables** — owned by the application, mutated by the
   application or by external CDC sources.
2. **A DAG of stream tables** — declared with
   `pgtrickle.create_stream_table(...)`, each holding the materialized
   result of a SQL query, maintained by differential refresh against
   change buffers.
3. **Downstream consumers** — application reads, dashboards, exports,
   reverse-ETL.

The DAG in (2) is the asset that earns its keep. It is also the asset
that is hardest to evolve safely. Five concrete pain points:

### 1.1 Drop-and-recreate destroys differential state

Today, changing the SQL of a stream table in any non-trivial way
(`ALTER`, schema change to a base table, new aggregation column, new
join) requires `pgtrickle.drop_stream_table()` + recreate. This:

- discards the materialized rows (the consumers see an empty table for
  the duration of the rebuild);
- triggers a `FULL` refresh, which can take minutes-to-hours on a large
  base table;
- forces every *downstream* stream table to also be recreated in
  topological order — no tooling enforces this today;
- loses any tracked CDC offsets / WAL slot positions, depending on
  CDC mode.

For a small DAG with 5 nodes this is annoying. For a 200-node
production DAG it is an outage.

### 1.2 Topological order is the user's problem

If a user writes a SQL change to node `B` whose query depends on node
`A`, and they want to also widen `A`, today they must figure out the
order: change `A`, refresh `A`, change `B`, refresh `B`. Get it wrong
and `B`'s SQL fails to parse or returns wrong data. There is no
tooling that says "the desired state has these 7 changes; here is the
plan; here is the dependency order."

### 1.3 Base-table ALTERs silently break downstream queries

`pg_trickle` validates a stream-table query at *create* time and at
DDL-event-trigger time. But base-table evolution (a column rename, a
type widening, a dropped not-null constraint) outside of `pg_trickle`'s
own DDL hooks can leave the DAG in an inconsistent state where the
next refresh fails. Today the only response is to drop and recreate
the stream tables. There is no *plan/preview* phase that says "this
ALTER will break the following 14 downstream nodes — here is the
patched SQL we propose."

### 1.4 No declarative source of truth

Stream tables today live in `pgtrickle.pgt_stream_tables` (a catalog
inside the database). The *imperative* `SELECT
pgtrickle.create_stream_table(...)` calls used to set them up may live
in a SQL file in the user's repo, in a dbt model, in a one-off psql
session, or in Liquibase — `pg_trickle` does not care. There is no
canonical mapping from "what is in git" to "what is in the database",
and no `aqueduct plan` / `aqueduct apply` cycle to reconcile the two.

### 1.5 Environment promotion is manual

Promoting a DAG change from dev → staging → prod is currently a
hand-rolled sequence of psql calls. Branch-based preview environments
(create a copy of the DAG with a different schedule and a smaller
sample of base data) are unsupported.

---

## 2. What `pg_aqueduct` Is

A **declarative migration and orchestration tool**, distributed as:

- a **Rust CLI binary** (`aqueduct`) — the primary user surface;
- a thin **Postgres extension** (`pg_aqueduct`) — owns its own catalog
  (`aqueduct.migrations`, `aqueduct.locks`, `aqueduct.snapshots`,
  `aqueduct.dag_versions`);
- a **TOML/YAML project format** plus a folder of `*.sql` files;
- optional **GitHub / GitLab CI integrations** — `aqueduct plan` as a
  PR check, just like `terraform plan`.

The user-facing model is intentionally close to **Atlas** (for SQL
schema) and **Terraform** (for infrastructure):

```
$ aqueduct init                    # scaffold a project
$ aqueduct plan --to prod          # diff desired vs actual; produce a plan
$ aqueduct apply --to prod         # execute the plan, record migration
$ aqueduct status --to prod        # show drift, last migration, lag
$ aqueduct rollback --to prod      # revert to the previous DAG version
$ aqueduct preview --branch feat-x # spin up a preview DAG on a branch DB
```

The CLI talks to PostgreSQL through a normal `libpq` connection (no
custom protocol). The companion extension is required only for the
*online* migration features (§4.4); the basic plan/apply loop works
against any cluster that has `pg_trickle` installed.

---

## 3. Why It Does Not Belong in dbt

dbt is the obvious comparison; the problem looks superficially like
"dbt for stream tables". It is not, and the project would be ill-served
by being implemented as a dbt package.

| Dimension | dbt | `pg_aqueduct` |
|---|---|---|
| Execution model | **Build** — recompute models per `dbt run` | **Migrate** — diff DAG, apply minimum-disruption transitions |
| State model | Stateless — model output is the artefact | Stateful — preserve materialized rows and refresh metadata across schema changes |
| Failure model | Recompute the failed model from scratch | Roll back to a known DAG version, restore prior catalog state |
| Schedule | External (cron, Airflow, dbt Cloud) | Schedule lives *inside* the stream table; tool only edits it |
| Audience | Analytics engineers | Platform / database / SRE engineers |
| Granularity | Model-level | DAG-level: dependency-aware, cross-node consistency |
| Online evolution | Not in scope | First-class: ALTER-in-place where the IVM math allows |
| Source of truth | Models + manifest | Migrations directory + recorded version history |
| Concurrency | Single `dbt run`, no DB-side locks | Distributed lock in `aqueduct.locks`, multi-writer safe |

The existing **dbt-pgtrickle** package gives dbt users a
`materialized='stream_table'` macro. That is the *right* level of
integration with dbt. `pg_aqueduct` is a different tool for a different
audience: it is invoked by the platform team that operates the stream
DAG, not by the analytics engineer that authors models.

The two compose cleanly: dbt-pgtrickle generates the SQL, then
`aqueduct plan` reads the dbt-compiled artefacts and produces a
migration. (Detailed dbt interop in §6.)

---

## 4. Why It Does Not Belong in `pg_trickle`

`pg_trickle`'s scope is the **runtime engine**: change capture,
differential refresh, scheduling, validation of a single SQL transition.
Adding a project-level orchestrator into the extension would:

1. **Bloat the extension's surface** — `pg_trickle` deliberately stays
   below a dozen public SQL functions. A migration tool needs file I/O,
   git integration, plan/apply state, and CLI ergonomics that have no
   place in a `LANGUAGE C` Postgres extension.
2. **Conflate two trust boundaries** — runtime correctness vs. CI/CD
   tooling. Bugs in a migration planner should never destabilise a
   live refresh loop.
3. **Force a release cadence mismatch** — `pg_trickle` ships when the
   IVM engine changes. A migration tool ships when CI integrations,
   file-format changes, or new diffing strategies land. Coupling them
   slows both.
4. **Block multi-version targeting** — `aqueduct` should be able to
   apply migrations against `pg_trickle` 0.58, 1.0, and 1.x clusters
   alike. That is much easier from a separately-versioned binary than
   from inside the extension itself.
5. **Repeat the `pg_tide` lesson** — see
   [PLAN_RELAY_STANDALONE.md](PLAN_RELAY_STANDALONE.md). The relay
   was extracted for the same reasons; the architecture there is the
   template here.

The right shape is the same as `pg_tide`: a separate repository, a
thin Postgres extension that owns nothing more than its own catalog,
and a Rust binary that does the real work over `libpq`.

---

## 5. Use Cases

### 5.1 GitOps for the stream-table DAG

The contents of `migrations/` live in git. A PR that changes
`order_totals.sql` triggers `aqueduct plan` in CI, which posts a
human-readable diff (added/removed/changed columns; affected
downstream nodes; predicted refresh-mode change; estimated cost) into
the PR. Merging the PR triggers `aqueduct apply` against staging.
Promotion to prod is a separate manual gate.

### 5.2 Online schema evolution

A user adds a column to a base table. `aqueduct plan` detects that 4
downstream stream tables would inherit the new column; it generates a
plan that:

- runs the `ALTER TABLE` on the base table;
- runs `pgtrickle.alter_stream_table()` on each affected node *in
  topological order*;
- where the change is provably mergeable (a passthrough column, a new
  optional aggregate), keeps the materialized rows in place; otherwise
  schedules a `FULL` rebuild during a configured maintenance window.

### 5.3 Blue/green DAG deployment

For a large structural change (a node is split into two; an aggregate
key changes), `aqueduct deploy --strategy blue-green`:

1. creates the new DAG nodes alongside the old, in a parallel schema
   (`green_v17`);
2. backfills them in the background;
3. atomically swaps consumer-facing views once the green DAG is caught
   up;
4. retires the blue DAG after a configurable grace period.

### 5.4 Drift detection

`aqueduct status` continuously reports any divergence between the
catalog and the migrations directory: a manually-created stream table,
a hand-edited schedule, an out-of-band `ALTER`. CI can fail a build if
drift is detected.

### 5.5 Preview environments per pull request

`aqueduct preview --branch feat-x` builds a sampled copy of the DAG in
a scratch schema (or a scratch database / Neon branch / CloudNativePG
clone) so that reviewers can `EXPLAIN` and benchmark a candidate
change without touching prod.

### 5.6 Time-travel rollback

Every `apply` writes a snapshot of the prior DAG definition into
`aqueduct.dag_versions`. `aqueduct rollback --to v17` reconstructs the
prior topology and migrates back, preserving rows where possible.

### 5.7 Cross-environment promotion with parameterisation

The same migration directory targets dev, staging, and prod with
environment-scoped variables (`schedule = '5m'` in dev, `'30s'` in
prod; `cdc_mode = 'trigger'` everywhere except a Citus-backed prod
where it is `'wal'`). `pg_aqueduct` owns the substitution.

---

## 6. Composition With Adjacent Tools

| Tool | Relationship |
|---|---|
| **`pg_trickle`** | Runtime target. `aqueduct` calls its SQL API. |
| **`pg_tide`** | Sibling — the relay, outbox, inbox. `aqueduct` may manage tide's catalog (relay subscriptions, inbox shapes) under the same migration discipline. |
| **dbt / dbt-pgtrickle** | Upstream authoring. `aqueduct ingest --from dbt-target` reads dbt's compiled `manifest.json` and produces an `aqueduct` migration. |
| **Atlas / Liquibase / sqitch** | Complementary — they own general-purpose base-table schema migrations (`CREATE TABLE`, partitioning, index changes). `pg_aqueduct` owns *stream-adjacent* base-table changes — any ALTER to a column that is a source for at least one stream table. For those changes, aqueduct generates and executes the base-table ALTER **and** the downstream stream-table cascade in a single coordinated plan. For standalone base-table migrations, aqueduct can invoke Atlas/sqitch as a pre-step hook. |
| **Terraform / Pulumi** | Outer — provisions the database; the operator embeds a `terraform_data` resource that calls `aqueduct apply` post-provision. |
| **CloudNativePG / Patroni** | `aqueduct` understands HA: it locks against the primary, refuses to apply against a standby, and integrates with switchover events. |
| **GitHub / GitLab Actions** | First-class CI integration: `aqueduct/plan-action`, `aqueduct/apply-action`. |

---

## 7. Architecture

### 7.1 Repository layout (proposed)

```
trickle-labs/pg-aqueduct/
├── README.md
├── Cargo.toml                        # workspace
├── crates/
│   ├── aqueduct-core/                # planner, differ, plan executor
│   ├── aqueduct-cli/                 # `aqueduct` binary
│   ├── aqueduct-extension/           # pgrx extension (catalog only)
│   └── aqueduct-testkit/             # shared Testcontainers helpers
├── examples/
│   ├── minimal/                      # 3-node DAG
│   ├── tpch/                         # 22 stream tables from TPC-H Q1–Q22
│   └── medallion/                    # bronze/silver/gold pattern
├── docs/
└── tests/
    ├── e2e_*.rs                      # against pg_trickle + pg_aqueduct
    └── property/                     # roundtrip plan→apply→plan = empty
```

### 7.2 Project file format (v0.1)

```toml
# aqueduct.toml — at the project root
[project]
name = "checkout-analytics"
version = "1"

[targets.dev]
dsn  = "postgresql://localhost/checkout_dev"
vars = { schedule = "5m", cdc_mode = "trigger" }

[targets.prod]
dsn  = "${AQUEDUCT_PROD_DSN}"
vars = { schedule = "30s", cdc_mode = "wal" }

[apply]
lock_timeout         = "30s"
maintenance_window   = "02:00-04:00 UTC"
allow_full_refresh   = false        # require online-evolution path
default_strategy     = "in-place"   # or "blue-green"
```

```sql
-- migrations/streams/order_totals.sql
-- @aqueduct:depends_on  = ["raw.orders"]
-- @aqueduct:schedule    = "{{ var.schedule }}"
-- @aqueduct:refresh_mode = "DIFFERENTIAL"
SELECT
    customer_id,
    SUM(amount) AS total_amount,
    COUNT(*)    AS order_count
FROM raw.orders
GROUP BY customer_id;
```

Base tables that are *sources* for stream tables may also be declared
in the migrations directory, so that `aqueduct plan` can compute the
full impact of a base-table change on the downstream DAG:

```sql
-- migrations/sources/orders.sql
-- @aqueduct:kind = source
-- @aqueduct:owned = false    -- aqueduct tracks schema but does not create the table
CREATE TABLE raw.orders (
    id          bigint PRIMARY KEY,
    customer_id bigint NOT NULL,
    amount      numeric(12,2) NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now()
);
```

`owned = false` (the default for sources) means `aqueduct apply` will
never `CREATE` or `DROP` the base table — it only reads its schema to
compute stream-table cascade plans. `owned = true` tells aqueduct it
may emit base-table DDL as part of a coordinated migration (useful for
full GitOps environments where the entire schema lives in one repo).

The format is intentionally simple — a folder of SQL files with
front-matter directives — so that diffs in PRs are readable and so
that interop with dbt-compiled artefacts is trivial.

### 7.3 Diffing strategy

`aqueduct plan` produces a plan by walking three sources of truth:

1. **Desired state** — parse the migrations directory; build a target
   DAG.
2. **Actual state** — query `pgtrickle.pgt_stream_tables` and the
   underlying `pg_class` for the live DAG.
3. **Recorded history** — read `aqueduct.dag_versions` to know the
   *intended* current state (which may diverge from actual = drift).

The differ operates over three layers simultaneously:

1. **Source layer** — base tables and views that the stream-table DAG
   reads from. For `owned = false` sources, aqueduct tracks schema
   changes from `pg_attribute` and uses them for impact analysis only.
   For `owned = true` sources, it also generates base-table DDL.
2. **Stream-table DAG** — the nodes and edges managed by `pg_trickle`.
3. **Consumer layer** (v1.1+) — materialized views, API views, or
   export connectors that read from stream tables.

Changes at layer 1 cascade into layer 2 automatically. The key
classification question is: *does this source-layer change affect any
column referenced in a stream-table query?* If yes, aqueduct owns the
full cascade plan. If no, it delegates to Atlas/sqitch/Liquibase.

Each stream-table delta is classified into one of four migration kinds:

| Class | Example | Cost |
|---|---|---|
| **Free** | Schedule change, refresh-mode change, CDC mode change | A single `pgtrickle.alter_stream_table()` call; no rebuild |
| **In-place** | Add a passthrough column, add an aggregate, widen a type | `ALTER` + targeted incremental backfill; preserves state |
| **Rebuild** | Change `GROUP BY` keys, change a join condition | Drop + recreate + `FULL` refresh |
| **Blue/green** | Restructure the topology of a sub-DAG | Build green alongside blue, swap atomically |

The classifier is the project's intellectual core. Many of the
"in-place" cases require the **same delta-rule reasoning** that
`pg_trickle`'s DVM operators already do — there is a real opportunity
to extract that logic into a shared crate (`pg_trickle_calculus`) that
both projects depend on.

### 7.4 Plan execution

A plan is an ordered list of *steps*; each step is one of:

- `LockDag { ttl }`
- `AlterBaseTable { name, statement }`
- `CreateStreamTable { spec }`
- `AlterStreamTable { name, change }`
- `DropStreamTable { name, cascade }`
- `Backfill { name, mode }` (full or windowed)
- `SwapView { from, to }` (blue/green)
- `RecordSnapshot { version }`
- `WaitForRefresh { name, deadline }`
- `RunHook { name, statement }` (user-defined SQL pre/post)

Execution is transactional where possible (most `ALTER` paths fit in
one transaction); for steps that cannot be transactional (CONCURRENTLY
INDEX, large `FULL` refreshes), the planner emits a *resumable*
sequence — the plan stores its progress in `aqueduct.migrations` so
`aqueduct apply --resume` works after a crash.

### 7.5 The companion extension

`aqueduct-extension` is a small pgrx extension that owns:

```sql
CREATE SCHEMA aqueduct;

CREATE TABLE aqueduct.dag_versions (
    version    bigserial PRIMARY KEY,
    project    text      NOT NULL,
    spec_hash  bytea     NOT NULL,
    applied_at timestamptz NOT NULL DEFAULT now(),
    applied_by text      NOT NULL,
    plan_jsonb jsonb     NOT NULL,
    spec_jsonb jsonb     NOT NULL          -- full snapshot of the DAG
);

CREATE TABLE aqueduct.migrations (
    id          bigserial PRIMARY KEY,
    from_version bigint REFERENCES aqueduct.dag_versions(version),
    to_version   bigint REFERENCES aqueduct.dag_versions(version),
    started_at   timestamptz NOT NULL,
    finished_at  timestamptz,
    status       text NOT NULL,            -- 'running' | 'committed' | 'failed' | 'rolled_back'
    plan         jsonb NOT NULL,
    progress     jsonb NOT NULL DEFAULT '{}'::jsonb
);

CREATE TABLE aqueduct.locks (
    project     text PRIMARY KEY,
    holder      text NOT NULL,
    acquired_at timestamptz NOT NULL,
    ttl         interval NOT NULL
);
```

It exposes a handful of SQL functions (`aqueduct.acquire_lock`,
`aqueduct.release_lock`, `aqueduct.record_snapshot`,
`aqueduct.list_drift`) but does **no orchestration of its own** — the
CLI is the brain. Keeping the extension trivial keeps the upgrade
story trivial and lets us version the CLI freely.

### 7.6 Locking & concurrency

- **One project, one writer.** A row lock on `aqueduct.locks` (with
  TTL + heartbeat) prevents two simultaneous `aqueduct apply` runs.
- **Cooperation with `pg_trickle` scheduling.** `aqueduct apply` calls
  `pgtrickle.pause_scheduler()` for affected nodes for the duration of
  the migration, restoring on success or failure.
- **HA aware.** Refuses to apply against a hot standby; integrates
  with Patroni / CloudNativePG primary-promotion events.

---

## 8. Implementation Plan

### Phase 0 — Repository bootstrap (3 days)

- Create `trickle-labs/pg-aqueduct` (mirrors `trickle-labs/pg-tide`
  layout from [PLAN_RELAY_STANDALONE.md](PLAN_RELAY_STANDALONE.md)).
- Cargo workspace, `pgrx` 0.18 extension scaffold, `clap`-based CLI
  scaffold, `justfile`, CI matrix (Linux + macOS unit + Testcontainers
  integration), code-coverage gate.
- Mirror `pg_trickle`'s testing tiers: unit / integration / E2E.
- Document the project's scope & non-goals in `README.md` and
  `ESSENCE.md`.

### Phase 1 — Read-only plan (1.5 weeks)

Goal: `aqueduct plan` is useful even without `apply`.

- TOML project loader.
- Migrations-folder parser (SQL + front-matter directives).
- Live-state reader (queries `pgtrickle.pgt_stream_tables`,
  `pg_class`, `pg_attribute`).
- DAG differ producing a typed `Plan` value.
- Human-readable plan renderer (text, JSON, markdown — the markdown
  output is what CI posts to PRs).
- Drift report.

Exit criteria: against a 20-node DAG on a Testcontainers Postgres,
`aqueduct plan` produces a correct plan for the cartesian product of
{add, drop, change SQL, change schedule, change refresh_mode}.

### Phase 2 — Apply (in-place + rebuild) (2 weeks)

- Plan executor with the step set in §7.4.
- `aqueduct.locks` and the lock manager.
- Snapshot recording into `aqueduct.dag_versions`.
- `aqueduct apply --resume` resumes after a crash.
- Pause/resume integration with `pg_trickle` scheduler.
- `aqueduct rollback`.

Exit criteria: a randomised property test that runs N random
plan→apply→plan→apply cycles on a Testcontainers cluster and asserts
that every cycle ends with an empty plan.

### Phase 3 — Online evolution (2 weeks)

- The migration classifier (§7.3).
- Implement the "free" and "in-place" classes against current
  `pg_trickle` `alter_stream_table` capabilities.
- Identify the small extensions to `pg_trickle` needed to enable
  more in-place paths (e.g. ALTER to widen a column type without a
  full rebuild) — file these as separate `pg_trickle` issues.
- `aqueduct apply --dry-run --explain-cost` reports per-step
  estimated row counts and refresh duration.

Exit criteria: TPC-H example DAG (22 nodes) accepts a column-add and
schedule-change migration without any node going through `FULL`.

### Phase 4 — Blue/green and preview environments (3 weeks)

- Blue/green deployer: builds the new DAG in a parallel schema,
  backfills, swaps consumer views.
- `aqueduct preview --branch` integrations:
  - native: spin a scratch schema with sampled base data;
  - CloudNativePG: create a clone cluster (see
    [PLAN_CLOUDNATIVEPG.md](ecosystem/PLAN_CLOUDNATIVEPG.md));
  - Neon: branch via the Neon API
    (see [PLAN_NEON.md](ecosystem/PLAN_NEON.md)).

Exit criteria: a documented blue/green migration of a 5-node DAG with
zero consumer-visible downtime.

### Phase 5 — CI integrations & ergonomics (1 week)

- `aqueduct/plan-action` and `aqueduct/apply-action` GitHub Actions.
- GitLab CI templates.
- `aqueduct fmt` (canonicalise SQL + front-matter).
- `aqueduct lint` (warns about `FULL`-only changes, missing
  `depends_on`, schedule too aggressive for cost class, etc.).
- Pre-commit hook.

### Phase 6 — dbt interop (1 week)

- `aqueduct ingest --from dbt-target target/` consumes dbt's compiled
  `manifest.json` and `compiled/` SQL, emits an aqueduct migrations
  directory.
- Round-trip example with `dbt-pgtrickle`.

### Phase 7 — Hardening to v1.0 (3 weeks)

- Multi-environment promotion workflow (`aqueduct promote dev→prod`).
- Encrypted secret handling (aligns with `pg_tide` and `pg_trickle`
  secret model).
- `aqueduct status --watch` long-running drift watcher.
- HA integration with Patroni & CloudNativePG primary-failover.
- Fuzzing the planner against random DAG mutations.
- Migration cookbook: 30 worked examples for the 30 most common
  evolution patterns (rename column, split node, change aggregate,
  switch from FULL to DIFFERENTIAL, etc.).
- Public benchmark: time-to-apply for a 200-node DAG with a 5-node
  change set vs. drop/recreate.

---

## 9. Sequencing Against Other Roadmap Items

| Prerequisite | Source | Status |
|---|---|---|
| `pg_trickle.alter_stream_table` exists and is reasonably complete | shipped | ✅ |
| `pgtrickle.pgt_stream_tables` is the canonical catalog | shipped | ✅ |
| `pg_trickle.pause_scheduler()` SQL function | likely missing — file as a `pg_trickle` issue | ⚠ |
| `pg_trickle` exposes a stable JSON projection of a stream-table spec | needs design | ⚠ |
| `pg_tide` extracted to its own repo | [PLAN_RELAY_STANDALONE.md](PLAN_RELAY_STANDALONE.md) | 🟡 in-flight |
| Shared `pg_trickle_calculus` crate for in-place classifier | future refactor | 🔴 later |

`pg_aqueduct` does **not** need to wait for the `pg_trickle_calculus`
extraction — Phase 3 can call into a vendored copy of the classification
logic until the shared crate exists.

---

## 10. Risks & Open Questions

### 10.1 The "every IVM system needs this" trap

If `pg_aqueduct` is too tightly coupled to `pg_trickle`, it can never
serve `pg_ivm`, Materialize, RisingWave, or future `pg_trickle` forks.
The recommended posture: **the planner is generic, the executor is
pluggable**. v0.1 ships only the `pg_trickle` executor; the trait
boundary is designed so a `pg_ivm` executor can be added later without
restructuring the planner.

### 10.2 SQL parsing

Front-matter directives + SQL is convenient but does not give us a
parsed AST. For accurate diffing the planner needs a Postgres-grade
parser. Recommendation: depend on the `pg_query.rs` crate (libpg_query
bindings) — same approach `pg_trickle` already uses for query
analysis.

### 10.3 In-place classifier accuracy

The line between "in-place safe" and "needs rebuild" is the
intellectual hard part. Getting it wrong silently produces wrong
results — *exactly* the failure mode the project promises to prevent.
Mitigation:

- conservative defaults (when in doubt, classify as Rebuild);
- a `--strict` mode that refuses any unproven in-place path;
- property-based tests that compare in-place result to from-scratch
  rebuild.

### 10.4 Single-writer assumption

The lock model assumes one `aqueduct apply` per project at a time.
Multi-tenant Postgres (one cluster, many independent projects) needs a
project-scoped lock, which §7.5 already provides — but the operator
ergonomics need work.

### 10.5 Base-table migrations and non-stream-table objects

`pg_aqueduct` takes a two-tier stance:

**Tier 1 — stream-adjacent base-table changes** (a column that appears
in at least one stream-table query): `pg_aqueduct` owns the entire
operation. It generates the base-table ALTER, computes the cascade
impact on the downstream DAG, classifies each affected stream-table
node (free / in-place / rebuild / blue-green), and executes the full
sequence as a single resumable plan. This is non-negotiable: allowing
two separate tools to split this coordination reintroduces the §1.3
problem (silently broken queries).

**Tier 2 — standalone base-table objects** (new tables, non-referenced
columns, indexes, partitioning, types, GUCs): delegate to
Atlas/Liquibase/sqitch. `aqueduct apply` can invoke a user-supplied
pre/post hook that runs the delegate tool, then proceeds with its own
stream-table plan. Do not build a general-purpose schema migration
tool — that would be competing with Atlas on Atlas's home turf.

The boundary between tier 1 and tier 2 is determined automatically by
the query parser: if `pg_query.rs` can prove that a column is
unreferenced in any stream-table query, aqueduct defers; otherwise it
classifies it as tier 1. The `--tier1-only` flag restricts the apply
run to stream-adjacent changes only, for teams that want to keep
base-table DDL in Atlas regardless.

### 10.6 Naming

`pg_aqueduct` follows the water-motion lineage of `pg_trickle` (a
small steady drip), `pg_ripple` (propagating outward), `pg_tide`
(flowing in and out). An aqueduct **carries water across distance** —
a fitting metaphor for moving DAG state from one version, environment,
or cluster to another. **Keep the name.**

### 10.7 Ownership of the in-place evolution rules

If the evolution classifier lives in `pg_aqueduct`, it can drift from
`pg_trickle`'s actual capabilities. If it lives in `pg_trickle`, the
extension grows scope. The medium-term answer is a shared
`pg_trickle_calculus` crate (§9). The short-term answer is to ship a
vendored copy and add a CI job that diffs the two.

---

## 11. Non-Goals (v1.0)

- A general-purpose schema migration tool. `pg_aqueduct` manages
  stream-adjacent base-table changes (§10.5 Tier 1) but defers
  standalone DDL (non-referenced columns, new tables, indexes,
  partitioning, types) to Atlas or sqitch.
- A query authoring environment. Use dbt, Hex, or psql.
- A monitoring / alerting product. Use `pg_trickle`'s monitoring views
  + Grafana.
- A multi-database (cross-cluster) orchestrator. That is the territory
  of `pg_tide` + a CDC pipeline; `aqueduct` operates against one
  PostgreSQL target at a time. (Multiple targets *sequentially* are
  fine — multi-target *transactional* coordination is not in scope.)
- A replacement for dbt-pgtrickle. They compose.

---

## 12. Recommendation

Open the `trickle-labs/pg-aqueduct` repository now, but keep the work
**unfunded** until two upstream items ship:

1. The `pg_tide` extraction lands and we have a known-good template
   for "small companion extension + Rust binary".
2. `pg_trickle` exposes a JSON-stable projection of a stream-table
   spec (small change, useful regardless).

In the meantime, this document plus a stub README in the new
repository is sufficient to:

- start collecting feedback from users who hit any of the §1 pain
  points;
- attract a co-maintainer with platform / CI / Atlas-style background;
- decide whether the v0.1 should be open-source from day one or
  developed inside `trickle-labs` and released at v0.5.

The strategic case is strong: a stream-table DAG without a migration
tool is a *toy*. Every successful `pg_trickle` deployment hits the §1
problems by month three. Owning the answer is how `pg_trickle` becomes
operationally serious.
