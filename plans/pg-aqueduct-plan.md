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
> The recommended shape is a **standalone Rust CLI** (`aqueduct`),
> shipped from a new repository `trickle-labs/pg-aqueduct`, with an
> **optional companion Postgres extension** that adds DDL event
> triggers and SQL-callable diagnostics (see §10.7). The CLI reads a
> versioned directory of declarative `*.sql` and `aqueduct.toml` files,
> computes a *plan* (diff between desired and actual DAG state), and
> executes the plan against a target database in topologically-correct
> order, preserving materialized state wherever possible. It is to a
> stream-table DAG what **Atlas** is to a relational schema and what
> **Terraform** is to infrastructure.
>
> Estimated effort to a usable v0.1 (plan + apply + rollback +
> import): **~4 weeks** for one engineer (Phase 0 = 3 days, Phase 1
> = 1.5 weeks, Phase 2 = 2 weeks). v1.0 (online schema evolution,
> blue/green deployments, drift detection, CI integrations, optional
> extension) is a **~14 week** project (Phases 0–7).

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

- a **Rust CLI binary** (`aqueduct`) — the primary user surface and
  the mandatory component. Works against any Postgres cluster that
  has `pg_trickle` installed. No superuser required beyond what
  `pg_trickle` itself already needs.
- an **optional companion extension** (`pg_aqueduct`) — enables DDL
  event triggers for passive drift detection and SQL-callable
  diagnostic views. Not required for plan/apply/rollback. See §10.7
  for the full analysis of whether an extension is the right choice.
- a **TOML/YAML project format** plus a folder of `*.sql` files;
- optional **GitHub / GitLab CI integrations** — `aqueduct plan` as a
  PR check, just like `terraform plan`.

The user-facing model is intentionally close to **Atlas** (for SQL
schema) and **Terraform** (for infrastructure):

```
$ aqueduct init                    # scaffold a project + bootstrap catalog
$ aqueduct import --from prod      # bootstrap from an existing pg_trickle deployment
$ aqueduct plan --to prod          # diff desired vs actual; produce a plan
$ aqueduct apply --to prod         # execute the plan, record migration
$ aqueduct status --to prod        # show drift, last migration, lag
$ aqueduct rollback --to prod      # revert to the previous DAG version
$ aqueduct preview --branch feat-x # spin up a preview DAG on a branch DB
$ aqueduct destroy --to prod       # tear down the entire project's DAG
```

The CLI talks to PostgreSQL through a normal `libpq` connection (no
custom protocol). `aqueduct init` bootstraps the `aqueduct.` catalog
schema directly (plain tables, no extension needed) — the same
approach used by Atlas, Flyway, and Liquibase.

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

**Rollback semantics and lossless guarantee:**

Rollback is only guaranteed lossless within the *current migration
window*. The window closes at the next full refresh of any affected
node. Specifically:

- For **free** and **in-place** migrations (schedule change, column
  add): rollback is always lossless — the prior schema is restored
  and the materialized data conforms to both old and new schema.
- For **rebuild** migrations: rollback is lossless only if no full
  refresh has completed since the migration applied. Once the new
  data has replaced the old, the prior rows are gone. `aqueduct
  rollback` reports the lossless window expiry time (estimated from
  the schedule of the slowest affected node) and requires
  `--accept-data-loss` if the window has passed.
- For **blue/green** deployments: the blue schema is retained until
  `--blue-ttl` expires (default 1 hour). Rollback within this period
  simply swaps the consumer views back to blue and drops green. After
  expiry, rollback requires a new forward migration from the green
  state.

`aqueduct rollback` never rolls back base-table DDL (Tier 1 changes
from §10.5). The operator must supply a compensating Atlas migration
for any base-table changes.

### 5.7 Cross-environment promotion with parameterisation

The same migration directory targets dev, staging, and prod with
environment-scoped variables (`schedule = '5m'` in dev, `'30s'` in
prod; `cdc_mode = 'trigger'` everywhere except a Citus-backed prod
where it is `'wal'`). `pg_aqueduct` owns the substitution.

### 5.8 When NOT to use `pg_aqueduct`

The tool's value proposition is entirely dependent on the presence of
a stream-table DAG. Without one, it adds complexity with no benefit.

**Case A — Pure base-table schema, no stream tables, no `pg_tide`:**
Use Atlas, sqitch, or Liquibase. They are mature, well-documented,
and purpose-built for this exact problem. `pg_aqueduct`'s unique
contributions — topological ordering, differential state preservation,
DAG cascade analysis, CALCULATED schedule resolution — are
meaningless here. Adopting `pg_aqueduct` for a pure-base-table schema
is solving a problem that does not exist.

**Case B — `pg_tide` outbox/inbox + base tables, no stream tables yet:**
Atlas is still the right answer for the migration tooling. `pg_tide`
tables (`tide.outbox`, `tide.inbox`, relay subscriptions) are normal
application tables — they do not form a dependency DAG. `pg_aqueduct`
has no analytical leverage over them.

The *only* argument for `pg_aqueduct` in a `pg_tide`-only schema is
the **gateway scenario**: a team that starts with `pg_tide` event
patterns and has a concrete plan to add `pg_trickle` stream tables
within the next few months. Starting with `pg_aqueduct` means the
transition to a full stream-table DAG requires no tool switch — the
migrations directory simply gains stream-table SQL files over time.
This is a weak argument. The tool switch from Atlas to `pg_aqueduct`
is not painful; the tool complexity of `pg_aqueduct` before any stream
tables exist is real. **The honest recommendation is: use Atlas until
you have your first stream table, then evaluate.**

**Case C — `pg_ripple` knowledge graph, no user-defined stream tables:**
`pg_ripple` is a PostgreSQL 18 RDF triple-store (SPARQL 1.1, SHACL,
Datalog reasoning). It uses `pg_trickle` as an optional companion for
specific features (incremental SPARQL views, ExtVP statistics, live
auto-updating CONSTRUCT views, ER monitoring stream tables). If a
`pg_ripple` deployment does **not** include user-defined incremental
SPARQL views or custom analytics stream tables over VP tables, there
is no stream-table DAG and therefore no value in `pg_aqueduct`.
`pg_ripple`'s internal VP tables (one per RDF predicate) and its
built-in ER monitoring tables are lifecycle-managed by `pg_ripple`
itself — `pg_aqueduct` must not touch them. Scoping rule: **only
user-authored `pg_trickle` stream tables in a `pg_ripple` deployment
are in scope for `pg_aqueduct`.** See §7.3 for how VP tables appear
as `owned = false` sources.

**The real threshold:** `pg_aqueduct` earns its operational complexity
the moment the first stream table exists and that stream table's
defining query references a column that could change. Before that
moment, it is a solution to a future problem.

---

## 6. Composition With Adjacent Tools

| Tool | Relationship |
|---|---|
| **`pg_trickle`** | Runtime target. `aqueduct` calls its SQL API. |
| **`pg_ripple`** | Knowledge graph companion. `pg_ripple`'s VP tables (per-predicate RDF storage) appear as `owned = false` sources. User-authored incremental SPARQL views and custom analytics nodes are in scope; `pg_ripple`-managed tables (ER monitoring, KGE embeddings, derivations) are excluded. See §5.8 Case C and §7.3. |
| **`pg_tide`** | Sibling — the relay, outbox, inbox. `aqueduct` *may* manage tide's catalog (relay subscriptions, inbox shapes) under the same migration discipline when stream tables are also present. When `pg_tide` is used **without** any `pg_trickle` stream tables, Atlas is the better migration tool. See §5.8. |
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
│   ├── aqueduct-extension/           # pgrx extension (Phase 4; optional)
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

# No user-facing "version" field — aqueduct tracks DAG versions
# automatically in aqueduct.dag_versions (auto-incremented on apply).

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
-- NOTE: depends_on is optional. The planner extracts dependencies
-- from the SQL via pg_query.rs (see §10.2). Use depends_on only to
-- declare dependencies the parser cannot infer: functions, implicit
-- casts, dynamic SQL, or intentional ordering constraints between
-- nodes that share no SQL-level edge.
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
   **`pg_ripple` VP tables** (`_pg_ripple.*` per-predicate tables)
   always appear as `owned = false` sources — aqueduct never generates
   DDL for them, but it detects when a VP table gains or loses a
   column and flags any downstream stream-table queries that reference
   it. `pg_ripple`'s internal tables (`_pg_ripple.kge_embeddings`,
   `_pg_ripple.derivations`, ER monitoring tables created by
   `enable_er_monitoring()`, etc.) are excluded from the source layer
   entirely; they are managed by `pg_ripple` and must not be tracked.
2. **Stream-table DAG** — the nodes and edges managed by `pg_trickle`.
   In a `pg_ripple` deployment this includes only **user-authored**
   incremental SPARQL views and custom analytics nodes; not the
   built-in ER monitoring stream tables that `pg_ripple` creates
   internally.
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

**Query pre-validation:** Before classifying any new or changed
stream-table query, `aqueduct plan` runs two validation passes:

1. **Parse check** — `pg_query.rs` parses the SQL. A parse failure
   is a plan error: the migration is rejected before any step is
   emitted, with the parse error surfaced directly.
2. **IVM-supportability check** — the same rules `pg_trickle` applies
   at `create_stream_table` time (volatile functions, unsupported
   aggregates, non-deterministic expressions). If the query is not
   differentiable and `refresh_mode = 'DIFFERENTIAL'`, `aqueduct plan`
   reports this as a **plan error** (not a warning). The `--auto-
   downgrade-refresh-mode` flag allows the planner to automatically
   reclassify to `FULL` in this case.

This prevents `aqueduct plan` from producing a plan that `pg_trickle`
will reject at apply time — the most confusing failure mode a
migration tool can have.

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
- `RecreatePolicy { name, policy_sql }` (restore RLS policies lost in rebuild)
- `ValidateQuery { name, query }` (IVM-supportability pre-check; always first)

Execution is transactional where possible (most `ALTER` paths fit in
one transaction); for steps that cannot be transactional (CONCURRENTLY
INDEX, large `FULL` refreshes), the planner emits a *resumable*
sequence — the plan stores its progress in `aqueduct.migrations` so
`aqueduct apply --resume` works after a crash.

### 7.5 Catalog tables and the companion extension

The **CLI** creates and owns the catalog tables on `aqueduct init`
(no extension required):

```sql
CREATE SCHEMA IF NOT EXISTS aqueduct;

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
    progress     jsonb NOT NULL DEFAULT '{}'::jsonb,
    cli_version  text,                     -- diagnostic (see §10.13)
    plan_format_version int NOT NULL DEFAULT 1
);

CREATE TABLE aqueduct.locks (
    project     text PRIMARY KEY,
    holder      text NOT NULL,
    acquired_at timestamptz NOT NULL,
    ttl         interval NOT NULL
);

CREATE TABLE aqueduct.cluster_profile (
    key         text PRIMARY KEY,
    value_jsonb jsonb NOT NULL,
    measured_at timestamptz NOT NULL DEFAULT now()
);
```

This is the same approach used by Atlas (`atlas_schema_revisions`),
Flyway (`flyway_schema_history`), and Liquibase
(`databasechangelog`). No `CREATE EXTENSION` is needed for core
functionality.

#### The optional companion extension

The **optional** pgrx extension `pg_aqueduct` adds two capabilities
that cannot be replicated from outside the Postgres process:

1. **DDL event triggers** — a `ddl_command_end` event trigger that
   fires on every `ALTER TABLE` / `ALTER TYPE` and records the
   change in `aqueduct.ddl_log`. The CLI polls this table during
   `aqueduct status` to detect out-of-band schema changes without
   the user having to explicitly run `aqueduct plan`.
2. **SQL-callable diagnostics** — `SELECT * FROM aqueduct.drift()`,
   `SELECT * FROM aqueduct.plan_summary()`, `SELECT * FROM
   aqueduct.migration_history()` — useful in monitoring dashboards
   and psql sessions.

When the extension is absent, the CLI detects this and falls back to
polling-based drift detection (comparing `pg_attribute` snapshots
between runs). See §10.7 for the full architectural analysis.

#### Catalog self-migration ("migrate the migrator")

The catalog schema (`aqueduct.*` tables) will evolve across CLI
versions. The CLI handles this automatically:

1. **Version tagging.** The `aqueduct.cluster_profile` table stores
   the `aqueduct_catalog_version` key (an integer). This is set to
   `0` on `aqueduct init` and incremented each time the catalog
   schema is upgraded.
2. **Auto-upgrade on startup.** Every CLI command begins by comparing
   its compiled-in `CATALOG_SCHEMA_VERSION` constant against the
   value in the database. If the database is behind, the CLI applies
   the pending catalog migrations from its embedded SQL bundle — in
   a single transaction — before doing anything else.
3. **Embedded SQL bundle.** Catalog migrations live in
   `aqueduct-core/src/catalog/migrations/V{N}__description.sql`.
   They are embedded at compile time via `include_str!()` and
   shipped inside the binary. No external files are needed at
   runtime.
4. **Backward-incompatible catalog changes.** Any migration that
   removes a column or changes a type requires a `min_cli_version`
   guard in the catalog migration SQL. The CLI refuses to apply
   against a catalog version it does not understand and prints a
   clear upgrade path.
5. **Bootstrapping.** Because the CLI migrates its own catalog before
   doing anything else, there is no chicken-and-egg problem: a fresh
   `aqueduct init` runs all catalog migrations from `V0` onward,
   and an upgrade run only applies the deltas since the last known
   version.



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
- Cargo workspace with two crates: `aqueduct-core` and `aqueduct-cli`.
  No pgrx in this phase (see §10.7 — extension is added in Phase 4).
- `clap`-based CLI scaffold, `justfile`, CI matrix (Linux + macOS
  unit + Testcontainers integration), code-coverage gate.
- Bootstrap `aqueduct init` — creates `aqueduct.` schema + catalog
  tables via plain SQL over libpq.
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
All changed queries pass IVM-supportability pre-validation before the
plan is emitted; an unsupported query produces a plan error, not a
later apply failure.

### Phase 2 — Apply (in-place + rebuild) (2 weeks)

- Plan executor with the step set in §7.4.
- `aqueduct.locks` and the lock manager.
- Snapshot recording into `aqueduct.dag_versions`.
- `aqueduct apply --resume` resumes after a crash.
- Pause/resume integration with `pg_trickle` scheduler.
- `aqueduct rollback` (with lossless-window enforcement per §5.6).
- `aqueduct import` — bootstrap from the live catalog (§10.21).
  Ships alongside `apply` so early adopters can onboard immediately.
- `RecreatePolicy` step: detect and re-apply RLS policies dropped
  during rebuild-class migrations.

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

### Phase 4 — Blue/green, preview environments, and optional extension (3 weeks)

- Blue/green deployer: builds the new DAG in a parallel schema,
  backfills, swaps consumer views (see §10.9).
- `aqueduct preview --branch` integrations:
  - native: spin a scratch schema with sampled base data;
  - CloudNativePG: create a clone cluster (see
    [PLAN_CLOUDNATIVEPG.md](ecosystem/PLAN_CLOUDNATIVEPG.md));
  - Neon: branch via the Neon API
    (see [PLAN_NEON.md](ecosystem/PLAN_NEON.md)).
- **Optional pgrx companion extension** (`aqueduct-extension/` crate):
  DDL event trigger for passive drift detection + SQL-callable
  diagnostic views. The CLI auto-detects whether the extension is
  installed and upgrades its behaviour accordingly.

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
| `pg_tide` extracted to its own repo | [PLAN_RELAY_STANDALONE.md](PLAN_RELAY_STANDALONE.md) | 🟡 in-flight (not a blocker for v0.1 — see §10.7) |
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

### 10.7 Does `pg_aqueduct` need to be a PostgreSQL extension?

This is the most consequential early architecture decision. The
answer is: **no, a PostgreSQL extension is not required — but an
optional one buys meaningful capabilities**.

#### Four possible approaches

**A — Pure CLI, no extension (Atlas model)**

The CLI bootstraps its own catalog (`aqueduct.` schema + plain
tables) on first `aqueduct init`, exactly like Atlas, Flyway, and
Liquibase manage `schema_migrations`. All locking uses PostgreSQL
advisory locks or row locks. No `CREATE EXTENSION` required.

| Pros | Cons |
|---|---|
| Works on RDS, Azure Database, Supabase, Neon, any managed Postgres where installing custom extensions is restricted or requires a support ticket | No DDL event triggers — can't passively observe out-of-band schema changes; must poll |
| Single artifact to install and version — just the CLI binary | No SQL-callable diagnostics (no `SELECT * FROM aqueduct.drift()`) |
| No pgrx build pipeline | No `\dx` visibility; catalog tables look like application tables |
| No superuser required beyond what `pg_trickle` already needs | Cannot intercept `ALTER TABLE` automatically |
| Fastest possible adoption path | |

**B — CLI + optional companion extension (recommended)**

The CLI works fully without the extension (option A). Installing
`pg_aqueduct` additionally enables DDL event triggers and SQL-callable
diagnostic views — a progressive-enhancement model. The CLI detects
whether the extension is present and upgrades its behavior
accordingly. This is the approach reflected in §7.5.

| Pros | Cons |
|---|---|
| Maximum compatibility — works on every managed Postgres | Two artifacts to version; CLI must handle both code paths |
| DDL event triggers available for teams that can install the extension | Extension availability on managed services is uneven |
| SQL-callable diagnostics for dashboards and psql power users | pgrx build dependency adds CI complexity |
| Clean `DROP EXTENSION CASCADE` for full uninstall | |
| Progressive adoption: start with CLI, add extension when needed | |

**C — Extension required for online features (original plan §7.5)**

The extension is required for any *online* migration (in-place,
blue/green). Without it, only rebuild-class migrations are available.

| Pros | Cons |
|---|---|
| Clean capability boundary | Blocks adoption on managed services for the most valuable feature class |
| Simpler CLI code (no fallback path) | Managed Postgres users would be limited to the weakest migration mode — perverse incentive |

**D — Full extension, no separate CLI**

All orchestration lives inside the Postgres process — background
workers, shared memory, `pg_trickle`-style `pgtrickle.create_stream_table()`
API. This is the model `pg_trickle` itself follows.

| Pros | Cons |
|---|---|
| Deep Postgres integration | No file I/O, no git integration, no PR checks — all the things that make a migration tool useful |
| One `CREATE EXTENSION` installs everything | Conflates runtime and migration tooling (see §4 for why this was rejected for `pg_trickle` itself) |

#### Recommendation: Option B

**Ship a pure CLI first. Make the extension optional from day one.**

The core insight is that all the genuinely unique value of
`pg_aqueduct` — the DAG differ, topological plan, cascade classifier,
resumable executor, GitOps integration, CI checks — lives in the
CLI binary and requires nothing but `libpq`. The extension only
adds two capabilities that cannot be replicated from outside the
process: DDL event triggers and SQL-callable views. Both are
conveniences, not correctness requirements.

Shipping option A first (pure CLI, no extension) means:
- `pg_aqueduct` works on day one against every cloud Postgres
  (RDS, Supabase, Neon, Azure, AlloyDB, CloudNativePG, Citus);
- there is no pgrx build in the `pg_aqueduct` repository for v0.1
  (dramatic CI simplification, especially for macOS cross-compile);
- the extension can be added in Phase 4 as an enhancement, not
  a prerequisite.

The only things that *genuinely* require the extension:
- Passive DDL drift detection (event trigger on `ALTER TABLE`)
- SQL-callable diagnostic views exposed to dashboards / `psql`

Both of these are useful-but-not-required for the core plan/apply
cycle. Polling-based drift detection is a perfectly adequate fallback
for v0.1.

**Revised §8 Phase 0 note:** drop the pgrx scaffold from Phase 0.
The extension becomes a Phase 4 deliverable, after the plan/apply/rollback
cycle is solid. Phase 0 is just a Rust CLI workspace.

### 10.8 Ownership of the in-place evolution rules

If the evolution classifier lives in `pg_aqueduct`, it can drift from
`pg_trickle`'s actual capabilities. If it lives in `pg_trickle`, the
extension grows scope. The medium-term answer is a shared
`pg_trickle_calculus` crate (§9). The short-term answer is to ship a
vendored copy and add a CI job that diffs the two.

### 10.9 Diamond DAG consistency during migration

**Problem:** `pg_trickle` supports `diamond_consistency = 'atomic'`
— nodes that converge in a diamond topology are refreshed as an
atomic group (SAVEPOINT-protected). If `aqueduct plan` produces a
migration that touches nodes in such a diamond and assigns them
*different* migration classes (one is "free", another is "rebuild"),
the diamond's data consistency invariant can be violated: the free
node updates while the rebuild node is momentarily empty.

**Recommendation: diamond groups are always migrated as a unit.**

1. `aqueduct plan` detects diamond groups by querying
   `pgtrickle.pgt_stream_tables` for nodes that share
   `diamond_consistency = 'atomic'` membership (or by traversing
   the dependency graph for convergence nodes).
2. All nodes in a diamond group are assigned the **highest migration
   class** of any member. If one member is "rebuild", all members
   are treated as "rebuild" — they are dropped and recreated
   together. The plan renderer explains why: "node B upgraded to
   rebuild class because it shares diamond group G with node A
   (rebuild)."
3. The group is migrated atomically: `LockDag`, alter/rebuild all
   members, `RecordSnapshot`, `UnlockDag` — with no other nodes
   refreshing in between (the scheduler is paused for all group
   members simultaneously).
4. `aqueduct plan` rejects any migration that would place diamond
   group members into different schemas during a blue/green
   deployment. Blue/green migrations that include a diamond must
   move the entire diamond to the green schema together.

**Implication for §10.5 Tier 1 (base-table changes):** A Tier 1
alter to a column that feeds into a diamond node triggers the entire
diamond group to be upgraded to the same class. This is conservative
but safe.

### 10.10 Blue/green atomicity — how do consumers cut over?

**Problem:** When a blue/green deployment swaps the DAG from version N
to version N+1, consumer queries (application reads, dashboards,
exports) must switch atomically from the blue stream tables to the
green ones. If even one consumer reads a mix of blue and green tables
in a single query, the result is silently wrong.

**Recommendation: rename-swap behind stable views.**

The consumer view pattern described below is also the foundation of
consumer-layer management (§10.17). For non-blue/green migrations,
the view layer is optional (see §10.21 for adoption path).

1. Each stream table `foo` is consumed through a view `public.foo`
   (or whatever schema the consumer expects). The actual materialised
   table lives in a versioned schema: `aqueduct_v17.foo`.
2. Blue/green builds the green DAG in `aqueduct_v18.*`, backfills it,
   and waits for it to converge.
3. The cutover is a single transaction that executes `ALTER VIEW
   public.foo SET SCHEMA ...` or `CREATE OR REPLACE VIEW public.foo
   AS SELECT * FROM aqueduct_v18.foo` for every node in the DAG. This
   is an `ACCESS EXCLUSIVE` lock on the views — not on the
   underlying tables — so it is sub-millisecond.
4. After a configurable grace period (`--blue-ttl 1h`), `aqueduct
   apply --cleanup` drops the old versioned schema.

This is the same pattern used by `pg_deploy`, `sqitch`, and
large-scale zero-downtime migration tooling. The consumer-facing
views are the stable API; the underlying tables are versioned
implementation details.

**Alternative considered and rejected:** `SET search_path` at the
session level. This is fragile (every connection pool session must
be reconfigured), incompatible with connection poolers that strip
session state (PgBouncer in transaction mode), and does not provide
atomicity across multiple consumers.

### 10.11 Failure recovery and partial state

**Problem:** A migration can crash at any point — after the base-table
ALTER but before the stream-table cascade, after 3 of 7 stream tables
are rebuilt, or during a large backfill. What's the recovery model?

**Recommendation: checkpoint-based resumable execution.**

Every plan step writes its outcome to `aqueduct.migrations.progress`
(a JSONB column) before proceeding to the next step. On crash:

```bash
aqueduct apply --resume   # reads progress, skips completed steps
```

Three step categories with different recovery semantics:

| Category | Examples | Recovery |
|---|---|---|
| **Transactional** | `AlterStreamTable`, `SwapView`, `RecordSnapshot` | Rolled back automatically on crash — step is retried |
| **Idempotent** | `CreateStreamTable` (IF NOT EXISTS), `AlterBaseTable` (guarded by catalog check) | Safe to re-execute — step checks precondition before acting |
| **Long-running** | `Backfill { mode: FULL }`, `WaitForRefresh` | Checkpointed by row-range or by table. On resume, `Backfill` restarts the affected table's full refresh (not the entire plan). `WaitForRefresh` simply re-polls |

**Invariant:** no step may leave the database in a state where the
*old* plan version and the *new* plan version are both partially
applied without the checkpoint recording which steps completed.
Violation of this invariant is a data-loss bug.

**Worst case — unresumable state:** If the CLI cannot determine
whether a step completed (e.g., connection lost mid-`ALTER TABLE`
with no way to query the catalog), `aqueduct apply --resume` prints
a diagnostic report and exits with a non-zero code. The operator
must inspect and either `--force-retry` or `--force-skip` the
ambiguous step. Automatic guessing is not acceptable — this is the
"data loss is unacceptable" principle from `pg_trickle`'s AGENTS.md.

### 10.12 Concurrency contract with the `pg_trickle` scheduler

**Problem:** §7.6 says `aqueduct apply` calls
`pgtrickle.pause_scheduler()`, but what if a refresh is already
in-flight when the pause request arrives? What if pause fails?

**Recommendation: drain-then-pause protocol.**

1. `aqueduct apply` calls `pgtrickle.pause_scheduler(nodes => [...])`.
   This sets a flag in `pg_trickle`'s shared memory that prevents the
   scheduler from *starting* new refreshes for the listed nodes.
2. The CLI then polls `pgtrickle.pgt_stream_tables` for
   `refresh_status = 'running'` on the affected nodes, waiting up to
   `lock_timeout` (from `aqueduct.toml`) for in-flight refreshes to
   complete.
3. If the drain deadline expires with a refresh still running, the
   CLI aborts the migration cleanly (no changes applied) and reports
   which node is blocking.
4. On migration success *or* failure (including crash), the CLI calls
   `pgtrickle.resume_scheduler(nodes => [...])`. If the CLI crashes
   before resuming, the `aqueduct.locks` TTL expires and a subsequent
   `aqueduct apply --resume` or `aqueduct unlock` call resumes the
   scheduler.

**Prerequisite for `pg_trickle`:** expose `pause_scheduler()` and
`resume_scheduler()` SQL functions with per-node granularity. These
do not exist today — file as a `pg_trickle` issue (already noted in
§9).

**Fallback if `pause_scheduler()` is unavailable (older
`pg_trickle` versions):** the CLI sets each affected node's schedule
to `'999d'` (effectively infinite), applies the migration, then
restores the original schedule. This is racy but acceptable for
v0.1 against older clusters.

### 10.13 Cost estimation

**Problem:** `aqueduct apply --dry-run --explain-cost` promises
per-step estimated row counts and refresh duration, but how accurate
can these be?

**Recommendation: Postgres-native estimates + empirical calibration.**

1. **Row count:** Run `EXPLAIN (FORMAT JSON)` against the stream
   table's defining query with current statistics. Extract
   `Plan Rows` from the top-level node. This is free (no actual
   execution), available on every Postgres version, and accurate
   enough for order-of-magnitude estimates.
2. **Refresh duration:** For `FULL` rebuilds, estimate as
   `estimated_rows × bytes_per_row / measured_write_throughput`.
   The write-throughput constant is calibrated per-cluster: on
   first `aqueduct init`, run a small benchmark (INSERT 10k rows
   into a temp table, measure wall time). Store the result in
   `aqueduct.cluster_profile`. This is crude but within 2–5×.
3. **For `DIFFERENTIAL`:** estimate is "near-zero" (the existing
   materialised state is preserved). Report the estimated change-
   buffer size instead of duration.
4. **For `IN-PLACE`:** estimate is "ALTER + incremental backfill".
   Use the same row-count estimate but with a lower throughput
   constant (ALTER + backfill is cheaper than full rebuild).

**Display:** The plan renderer shows a summary table:

```
Step                          Rows (est)    Duration (est)    Class
─────────────────────────────────────────────────────────────────────
ALTER base raw.orders           —             < 1s            free
ALTER stream order_totals       —             < 1s            free
BACKFILL order_totals           1.2M          ~45s            rebuild
ALTER stream customer_summary   —             < 1s            in-place
BACKFILL customer_summary       340K          ~12s            in-place
```

**Non-goal:** sub-second accuracy. The purpose is to distinguish
"this migration takes 2 seconds" from "this migration takes 2 hours"
so the operator can decide whether to run it now or schedule it for
the maintenance window.

### 10.14 CLI upgrade mid-migration

**Problem:** If the CLI binary is upgraded while a resumable migration
is in-flight (e.g., operator deploys a new `aqueduct` version, then
runs `aqueduct apply --resume`), can it safely resume?

**Recommendation: plan format versioning + forward compatibility.**

1. Every serialised plan in `aqueduct.migrations.plan` includes a
   `plan_format_version` integer (starting at 1).
2. A CLI binary can resume any plan whose `plan_format_version` is
   ≤ its own compiled-in version. It refuses to resume a plan from a
   *newer* CLI with a clear error message.
3. Plan format changes are rare (they are structural, not cosmetic)
   and are always backwards-compatible additions, never removals or
   reinterpretations. If a breaking change is unavoidable, bump the
   major format version and reject old plans.
4. The CLI records its own version in
   `aqueduct.migrations.cli_version` for diagnostic purposes.

**Practical consequence:** upgrading the CLI mid-migration is safe
as long as the new CLI is the same or newer major version. Downgrading
is not supported (the old CLI may not understand new step types).

### 10.15 HA / failover integration

**Problem:** In an HA cluster (Patroni, CloudNativePG, Stolon), the
primary can change at any time. If `aqueduct apply` is mid-migration
when a failover happens, what occurs?

**Recommendation: detect failover, abort cleanly, resume on new
primary.**

1. On `aqueduct apply` startup, the CLI records the current primary's
   `system_identifier` + `timeline_id` (from `pg_control_system()`)
   in the migration row.
2. Between each plan step, the CLI re-checks that the connection is
   still to the same primary. If the `system_identifier` or
   `timeline_id` has changed (indicating a failover), the CLI:
   - marks the migration as `status = 'interrupted'` with the
     last completed step;
   - exits with a clear error: "Failover detected. Re-run
     `aqueduct apply --resume` against the new primary."
3. The lock row in `aqueduct.locks` has a TTL. If the CLI process
   dies in a failover, the lock expires and a fresh `aqueduct apply
   --resume` can proceed on the new primary.
4. `aqueduct apply` **refuses to run against a hot standby** (checks
   `pg_is_in_recovery()`). This is a hard error, not a warning.

**Patroni-specific:** The CLI can optionally accept a
`--patroni-endpoint` flag and use the Patroni REST API to discover
the current primary, avoiding stale DNS.

**CloudNativePG-specific:** For Kubernetes deployments, the CLI
can read the `cnpg.io/cluster` annotation to discover the current
primary service endpoint.

### 10.16 Schedule resolution during DAG restructuring

**Problem:** If a downstream node has `schedule = 'calculated'` and
its upstream dependencies are restructured (a new parent is added, a
parent is removed, a parent's schedule changes), how is the
calculated schedule resolved?

**Recommendation: `pg_trickle` owns schedule resolution; `aqueduct`
defers to it.**

`pg_trickle`'s `CALCULATED` schedule mode already resolves the
schedule from the DAG topology. `pg_aqueduct` does not need to
reimplement this. The contract is:

1. `aqueduct plan` reads the *current* calculated schedule from
   `pgtrickle.pgt_stream_tables.schedule_resolved` for display
   purposes.
2. `aqueduct apply` calls `pgtrickle.alter_stream_table()` or
   `pgtrickle.create_stream_table()` with `schedule => 'calculated'`.
   `pg_trickle` resolves the schedule from the live DAG topology at
   that point.
3. If the restructuring changes which upstream nodes feed a
   calculated node, `pg_trickle` recalculates automatically on the
   next scheduler tick.
4. The plan renderer shows the *predicted* new schedule (computed
   by the CLI's DAG differ using the same algorithm), with a caveat:
   "Actual calculated schedule will be resolved by pg_trickle at
   apply time."

**Edge case — circular calculated dependencies:** Impossible by
construction. `pg_trickle` already prevents cycles in the DAG.
`aqueduct plan` rejects any migration that would introduce a cycle
(by running the same topological-sort + cycle-detection as
`pg_trickle`'s `dag.rs`).

### 10.17 Version compatibility matrix

**Problem:** Which `pg_trickle` versions can a given `aqueduct` CLI
target? How is this tested?

**Recommendation: minimum-version pinning + CI matrix.**

1. `aqueduct` declares a **minimum supported `pg_trickle` version**
   in `aqueduct.toml` (default: the oldest version that exposes the
   JSON spec projection — likely v0.60 or v1.0).
2. On `aqueduct apply`, the CLI queries
   `pgtrickle.pgt_extension_version()` and checks it against the
   minimum. If the installed version is too old, it prints a clear
   error listing the missing capabilities.
3. The CLI is **forward-compatible by default:** it uses only the
   stable SQL API (`create_stream_table`, `alter_stream_table`,
   `drop_stream_table`, `refresh_stream_table`). It does not depend
   on internal catalog column layouts or undocumented functions.
4. CI runs the E2E test suite against a matrix of `pg_trickle`
   versions: `{latest, latest-1, minimum_supported}`. This catches
   regressions early.

**Version skew policy:** `aqueduct` v0.x supports `pg_trickle`
v0.{min}–v0.{latest}. After both projects reach 1.0, the policy
tightens to `aqueduct` 1.x supports `pg_trickle` 1.x (same major
version, any minor).

### 10.18 Consumer-layer management

**Problem:** §7.3 mentions a three-layer model with a "consumer
layer (v1.1+)" but does not define what this entails.

**Recommendation: v1.0 manages consumer views; v1.1+ manages sinks.**

**v1.0 scope — consumer views only:**

Consumer views are the `CREATE OR REPLACE VIEW public.foo AS SELECT
* FROM aqueduct_vN.foo` objects described in §10.9 (blue/green).
`aqueduct` creates and manages these views as part of every
migration. They are the stable API that applications query.

The migrations directory can declare consumer views explicitly:

```sql
-- migrations/consumers/api_orders.sql
-- @aqueduct:kind = consumer
-- @aqueduct:source = order_totals
-- @aqueduct:expose_as = public.api_orders
SELECT customer_id, total_amount FROM order_totals
WHERE total_amount > 0;
```

This lets the migration tool track which application queries depend
on which stream tables, and include them in impact analysis and
blue/green swap.

**v1.1+ scope — sinks (out of scope for v1.0):**

Sinks are external consumers: reverse-ETL to Kafka, S3/Iceberg
exports, webhook notifications, `pg_tide` outbox entries. Managing
these requires understanding external system APIs and is a different
problem class. Defer to `pg_tide` and purpose-built connectors.

### 10.19 Project scope definition — what is a "project"?

**Problem:** One migrations directory per project. But what is a
project? One DAG? Multiple DAGs? If a company has 50 independent
stream-table DAGs, are there 50 directories and 50 lock rows?

**Recommendation: one project = one logically-connected DAG.**

A project is the smallest set of stream tables where a change to any
member *could* cascade to another member. In practice this means:

1. **Connected component.** If stream table A depends (directly or
   transitively) on stream table B, they belong to the same project.
   Independent sub-DAGs that share no edges are separate projects.
2. **One `aqueduct.toml` per project.** A monorepo can contain
   multiple project directories, each with its own `aqueduct.toml`.
3. **One lock row per project.** `aqueduct.locks` is keyed by
   `project` name. Two independent projects can run `aqueduct apply`
   concurrently without interference.
4. **Shared base tables.** If two projects read from the same base
   table but have no stream-table edges between them, they are still
   separate projects. A base-table ALTER affects both projects
   independently — each project's `aqueduct plan` reports its own
   cascade.

**Naming convention:** The project name defaults to the directory
name but can be overridden in `aqueduct.toml`:

```toml
[project]
name = "checkout-analytics"   # used as the lock key and version namespace
```

**Scale guidance:**

| Team size | Typical pattern |
|---|---|
| Small (1–5 stream tables) | One project, one directory |
| Medium (10–50) | 2–5 projects, grouped by domain (orders, inventory, analytics) |
| Large (50–200+) | One project per bounded context; each project has its own `aqueduct.toml`, CI pipeline, and deployment cadence |

### 10.20 Security model

**Problem:** `pg_aqueduct` connects to production databases with
`ALTER TABLE`, `DROP`, and `CREATE` privileges. The CLI stores
connection strings in `aqueduct.toml` or environment variables.
There is no discussion of credential handling, least-privilege
roles, or audit logging.

**Recommendation: least privilege + env-only secrets + audit trail.**

1. **No secrets in files.** DSN values in `aqueduct.toml` should use
   environment variable references (`${AQUEDUCT_PROD_DSN}`). The CLI
   refuses to proceed if a plaintext password appears in a config
   file and `--allow-plaintext-password` is not explicitly set.
2. **Least-privilege role.** Document a recommended `aqueduct_admin`
   role that has:
   - `USAGE` and `CREATE` on the `aqueduct` schema;
   - `USAGE` on the `pgtrickle` schema (to call the SQL API);
   - `SELECT` on `pg_class`, `pg_attribute`, `pg_type` (for diffing);
   - `CREATE`, `ALTER`, `DROP` on stream-table schemas only;
   - **No superuser, no `CREATEROLE`, no replication.**
   The CLI checks on startup that it has the minimum required
   privileges and warns if it has *more* than needed.
3. **Audit trail.** Every `aqueduct apply` records the authenticated
   Postgres role, client IP (`inet_client_addr()`), CLI version, and
   full plan in `aqueduct.migrations`. This is queryable for
   compliance audits.
4. **`--dry-run` never mutates.** The `plan` and `status` commands
   open read-only transactions (`SET TRANSACTION READ ONLY`) to
   guarantee they cannot accidentally modify data.

### 10.21 IMMEDIATE mode stream tables during migration

**Problem:** Stream tables with `refresh_mode = 'IMMEDIATE'` use
synchronous, in-transaction, statement-level triggers instead of
asynchronous CDC + scheduled refresh. Migrating them has fundamentally
different semantics: the triggers must be dropped and recreated
atomically, and during migration there is a window where DML against
the source table may not be captured.

**Recommendation: migrate IMMEDIATE tables inside a serialisable
transaction.**

1. `aqueduct plan` classifies IMMEDIATE stream tables separately
   from deferred (DIFFERENTIAL/FULL) ones.
2. For free/in-place changes to IMMEDIATE tables, the entire
   migration (trigger drop + ALTER + trigger recreate) is executed
   inside a single `SERIALIZABLE` transaction. This guarantees no
   DML is lost — any concurrent DML blocks until the migration
   transaction commits.
3. For rebuild-class changes to IMMEDIATE tables, the plan inserts a
   `PauseImmediate { name }` step that temporarily switches the
   stream table to `DIFFERENTIAL` mode (with a short schedule),
   applies the rebuild, then switches back to `IMMEDIATE`. This
   avoids the long-held `SERIALIZABLE` lock but introduces a brief
   window of eventual consistency. The plan renderer warns: "Stream
   table `foo` will be temporarily non-immediate during rebuild
   (~estimated_duration)."
4. The `--no-immediate-downgrade` flag rejects any plan that would
   temporarily downgrade an IMMEDIATE table, forcing the operator
   to schedule the migration during a maintenance window instead.

### 10.22 Adoption path for existing `pg_trickle` users

**Problem:** Existing `pg_trickle` deployments have stream tables
created via ad-hoc `SELECT pgtrickle.create_stream_table(...)` calls.
There is no migrations directory, no `aqueduct.toml`, and no recorded
version history. How do they adopt `pg_aqueduct`?

**Recommendation: `aqueduct import` bootstraps from the live catalog.**

```bash
aqueduct import --from prod --output ./my-project/
```

This command:

1. Connects to the target database.
2. Queries `pgtrickle.pgt_stream_tables` for all stream tables.
3. Generates a `migrations/streams/*.sql` file for each stream
   table, with front-matter directives populated from the catalog
   (schedule, refresh_mode, cdc_mode, etc.).
4. Optionally generates `migrations/sources/*.sql` for each base
   table referenced by at least one stream table (`owned = false`).
5. Generates a skeleton `aqueduct.toml`.
6. Runs `aqueduct init` against the database (creates the `aqueduct.`
   catalog schema).
7. Records the current state as `dag_version = 1` (the baseline).

After `import`, the user's next `aqueduct plan` should produce an
empty plan (desired state = actual state). From this point, all
changes go through the normal plan/apply cycle.

**The consumer view layer (\u00a710.10) is opt-in.** Existing users who
query stream tables directly (`SELECT * FROM public.order_totals`)
do not need to adopt the view indirection for non-blue/green
migrations. The view layer is only introduced when the user first
runs a blue/green deployment.

### 10.23 `aqueduct destroy` — tearing down a project

**Problem:** There is no documented way to cleanly remove an entire
project's DAG, catalog entries, and lock rows.

**Recommendation: `aqueduct destroy --project <name> --to <target>`.**

1. Drops all stream tables owned by the project (in reverse
   topological order) via `pgtrickle.drop_stream_table()`.
2. Drops consumer views managed by the project.
3. Deletes the project's rows from `aqueduct.dag_versions`,
   `aqueduct.migrations`, and `aqueduct.locks`.
4. Does **not** drop the `aqueduct.` schema itself (other projects
   may share it).
5. Requires `--confirm` flag (or interactive prompt) — this is a
   destructive, irreversible operation.

---

## 11. Non-Goals (v1.0)

- **Pure base-table schema management without stream tables.** If there
  are no `pg_trickle` stream tables in the schema, use Atlas, sqitch,
  or Liquibase instead (see §5.8). `pg_aqueduct` adds no value in
  this case.
- **`pg_tide`-only schemas (no stream tables).** `pg_tide` outbox,
  inbox, and relay subscription tables are normal application tables;
  they do not form a dependency DAG. Atlas manages them fine. The
  gateway scenario (§5.8) is a marginal exception, not a
  recommendation.
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
**unfunded** until one upstream item ships:

1. `pg_trickle` exposes a JSON-stable projection of a stream-table
   spec (small change, useful regardless).

Note: **the `pg_tide` extraction is no longer a prerequisite** for
`pg_aqueduct` v0.1. Per §10.7, the v0.1 CLI has no pgrx component —
it is a pure Rust binary that bootstraps its own catalog over libpq,
like Atlas. The pgrx companion extension arrives in Phase 4 and can
borrow patterns from `pg_tide` at that point. Unblocking one item
(JSON spec projection) instead of two cuts the lead time significantly.

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
