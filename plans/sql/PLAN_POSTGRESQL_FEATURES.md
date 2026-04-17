# PostgreSQL Feature Opportunities for pg_trickle

> **Date:** 2026-04-17
> **Scope:** PostgreSQL 18 (current target), PostgreSQL 19 (upcoming), and
> hypothetical future features relevant to pg_trickle's IVM engine, CDC
> pipeline, scheduling, and SQL coverage.

---

## Table of Contents

- [Executive Summary](#executive-summary)
- [PostgreSQL 18 Features — Already Available](#postgresql-18-features--already-available)
  - [HIGH: OLD/NEW in RETURNING for MERGE](#high-oldnew-in-returning-for-merge)
  - [HIGH: Asynchronous I/O Subsystem](#high-asynchronous-io-subsystem)
  - [HIGH: Virtual Generated Columns](#high-virtual-generated-columns)
  - [HIGH: NOT ENFORCED Constraints](#high-not-enforced-constraints)
  - [MEDIUM: Skip Scan for B-Tree Indexes](#medium-skip-scan-for-b-tree-indexes)
  - [MEDIUM: PG_MODULE_MAGIC_EXT](#medium-pg_module_magic_ext)
  - [MEDIUM: Extension Cumulative Statistics API](#medium-extension-cumulative-statistics-api)
  - [MEDIUM: Custom EXPLAIN Options for Extensions](#medium-custom-explain-options-for-extensions)
  - [MEDIUM: Temporal Constraints (WITHOUT OVERLAPS)](#medium-temporal-constraints-without-overlaps)
  - [LOW: UUIDv7](#low-uuidv7)
  - [LOW: Improved Hash Join / GROUP BY Performance](#low-improved-hash-join--group-by-performance)
  - [LOW: Logical Replication of Generated Columns](#low-logical-replication-of-generated-columns)
  - [LOW: extension_control_path](#low-extension_control_path)
  - [LOW: Idle Replication Slot Timeout](#low-idle-replication-slot-timeout)
- [PostgreSQL 19 Features — Upcoming](#postgresql-19-features--upcoming)
  - [HIGH: Automatic wal_level=logical Enablement](#high-automatic-wal_levellogical-enablement)
  - [HIGH: SQL/PGQ — Property Graph Queries](#high-sqlpgq--property-graph-queries)
  - [HIGH: REPACK CONCURRENTLY](#high-repack-concurrently)
  - [HIGH: INSERT ... ON CONFLICT DO SELECT ... RETURNING](#high-insert--on-conflict-do-select--returning)
  - [HIGH: Window Functions IGNORE NULLS / RESPECT NULLS](#high-window-functions-ignore-nulls--respect-nulls)
  - [HIGH: GROUP BY ALL](#high-group-by-all)
  - [HIGH: UPDATE/DELETE FOR PORTION OF (Temporal DML)](#high-updatedelete-for-portion-of-temporal-dml)
  - [MEDIUM: NOTIFY Optimization — Wake Only Listening Backends](#medium-notify-optimization--wake-only-listening-backends)
  - [MEDIUM: Planner Hooks (planner_setup_hook / planner_shutdown_hook)](#medium-planner-hooks-planner_setup_hook--planner_shutdown_hook)
  - [MEDIUM: SRF Replacement by Extensions](#medium-srf-replacement-by-extensions)
  - [MEDIUM: Logical Replication Sequence Sync](#medium-logical-replication-sequence-sync)
  - [MEDIUM: PUBLICATION EXCEPT Clause](#medium-publication-except-clause)
  - [LOW: COPY TO FORMAT JSON](#low-copy-to-format-json)
  - [LOW: pg_plan_advice / pg_stash_advice](#low-pg_plan_advice--pg_stash_advice)
  - [LOW: Improved ANTI JOIN Optimization](#low-improved-anti-join-optimization)
  - [LOW: CHECKPOINT FLUSH_UNLOGGED](#low-checkpoint-flush_unlogged)
- [Hypothetical / Future Features to Watch](#hypothetical--future-features-to-watch)
  - [Core IVM in PostgreSQL](#core-ivm-in-postgresql)
  - [Table Access Method (TAM) for Change Buffers](#table-access-method-tam-for-change-buffers)
  - [Native LISTEN/NOTIFY Improvements](#native-listennotify-improvements)
  - [Parallel DML (INSERT/UPDATE/DELETE/MERGE)](#parallel-dml-insertupdatedeletemerge)
  - [Incremental Sort for MERGE](#incremental-sort-for-merge)
  - [Column-Store / Hybrid Storage](#column-store--hybrid-storage)
  - [Pluggable WAL Decoders (In-Process)](#pluggable-wal-decoders-in-process)
  - [Event Trigger for DML](#event-trigger-for-dml)
- [Impact Matrix](#impact-matrix)

---

## Executive Summary

This report identifies PostgreSQL features across three horizons that
pg_trickle could leverage or must adapt to:

1. **PG 18 (current)** — Features already available but not yet exploited.
   Key opportunities: `OLD`/`NEW` in `MERGE RETURNING`, the AIO subsystem,
   virtual generated columns, and the extension statistics API.

2. **PG 19 (upcoming, ~Sep 2026)** — Features in the development branch.
   Key opportunities: automatic `wal_level=logical`, SQL/PGQ graph queries,
   `REPACK CONCURRENTLY`, `INSERT ... ON CONFLICT DO SELECT`, window function
   `IGNORE NULLS`, `GROUP BY ALL`, temporal DML, and targeted NOTIFY wakeups.

3. **Hypothetical / long-term** — Features under active discussion or that
   pg_trickle could implement internally ahead of PostgreSQL core. Key items:
   core IVM (competitive threat/opportunity), parallel DML, custom table
   access methods for change buffers, and pluggable in-process WAL decoding.

---

## PostgreSQL 18 Features — Already Available

### HIGH: OLD/NEW in RETURNING for MERGE

**What:** `INSERT`/`UPDATE`/`DELETE`/`MERGE` `RETURNING` clauses can now
reference `OLD.*` and `NEW.*` to return both pre-image and post-image rows.

**Opportunity for pg_trickle:**
- The delta application phase currently uses `MERGE` to apply diffs to
  storage tables. With `OLD`/`NEW` in `RETURNING`, pg_trickle could capture
  the exact rows displaced during `MERGE` in a single round-trip, eliminating
  the need for a separate pre-refresh snapshot when computing ST-to-ST change
  buffers.
- For the full-refresh → ST change buffer path, the current `EXCEPT ALL` diff
  can be replaced by `MERGE ... RETURNING OLD.*, NEW.*`, which produces the
  exact insert/delete pairs in one pass.
- Could simplify the `append_only` fast-path validation by inspecting the
  `RETURNING` output for unexpected `OLD` rows (meaning an update/delete was
  detected).

**Effort:** Medium — requires reworking `build_merge_sql()` and the
ST-change-buffer writer in `src/refresh.rs`.

---

### HIGH: Asynchronous I/O Subsystem

**What:** PG 18 adds an AIO subsystem (`io_method`) that allows backends to
queue multiple read requests for sequential scans, bitmap heap scans, etc.

**Opportunity for pg_trickle:**
- The delta scan CTEs read from change buffer tables, which can be large for
  high-throughput sources. AIO will automatically benefit these sequential
  scans without any code changes.
- The full-refresh path (truncate + insert from defining query) scans entire
  source tables — AIO speeds this up for free.
- **Documentation opportunity:** recommend `io_method = io_uring` (Linux) in
  the CONFIGURATION.md for best refresh throughput.
- **Benchmark opportunity:** re-run the E2E benchmarks with AIO enabled to
  quantify the improvement and publish in BENCHMARK.md.

**Effort:** Low — mostly documentation and benchmarking. No code changes
needed to benefit.

---

### HIGH: Virtual Generated Columns

**What:** Generated columns can now be `VIRTUAL` (computed on read, no
storage). This is the new default.

**Opportunity for pg_trickle:**
- **CDC impact:** Virtual generated columns have no physical storage, so they
  produce no `old_*`/`new_*` values in row-level triggers. pg_trickle's
  trigger-based CDC must handle this correctly — virtual columns should be
  excluded from change buffer schemas, and the defining query should reference
  the expression directly.
- **Selective CDC:** The `resolve_referenced_column_defs()` function must
  filter out virtual generated columns from the buffer schema.
- **Stream table storage:** If a stream table's defining query produces
  virtual generated column references, the storage table could use virtual
  generated columns itself to save space.

**Effort:** Medium — requires CDC column resolution changes and E2E tests
with virtual generated column sources.

---

### HIGH: NOT ENFORCED Constraints

**What:** `CHECK` and `FOREIGN KEY` constraints can now be declared
`NOT ENFORCED`, meaning the constraint metadata exists but is not validated.

**Opportunity for pg_trickle:**
- pg_trickle's storage tables could use `NOT ENFORCED` foreign keys to
  document the relationship between stream tables and their sources without
  incurring FK check overhead during `MERGE`/`INSERT`/`DELETE` delta
  application.
- The `NOT ENFORCED` FK metadata can be used by the optimizer for join
  elimination, potentially improving delta query plans.
- The `__pgt_count` column could have a `NOT ENFORCED CHECK (__pgt_count > 0)`
  constraint to document the invariant without runtime cost.

**Effort:** Low — add constraints during `CREATE EXTENSION` or stream table
creation.

---

### MEDIUM: Skip Scan for B-Tree Indexes

**What:** Multi-column B-tree indexes can now use "skip scan" when the
leading column has no restriction but later columns do.

**Opportunity for pg_trickle:**
- The covering indexes created by the Index-Aware MERGE Planning feature
  (A-4) use multi-column indexes. Skip scan could improve delta lookups when
  the first column is not in the WHERE clause of the delta query.
- Change buffer tables index on `(change_id)` and scan by LSN range —
  adding a multi-column index `(source_relid, change_lsn)` could benefit
  from skip scan for multi-source queries.

**Effort:** Low — evaluate via `EXPLAIN` on existing delta queries and add
indexes if beneficial.

---

### MEDIUM: PG_MODULE_MAGIC_EXT

**What:** New macro allowing extensions to report their name and version,
accessible via `pg_get_loaded_modules()`.

**Opportunity for pg_trickle:**
- pg_trickle should adopt `PG_MODULE_MAGIC_EXT` to expose its version through
  the standard PostgreSQL interface. This helps monitoring tools, cloud
  providers, and support workflows identify pg_trickle's exact version.
- pgrx may need to support this macro first.

**Effort:** Low — once pgrx supports it, a one-line change in `lib.rs`.

---

### MEDIUM: Extension Cumulative Statistics API

**What:** Extensions can now use the server's cumulative statistics
infrastructure to publish custom metrics.

**Opportunity for pg_trickle:**
- Replace the ad-hoc refresh statistics tracking (shared memory counters +
  custom views) with proper pg_stat integration.
- Publish per-stream-table metrics (refresh count, delta rows, latency) as
  cumulative statistics that survive `pg_stat_reset()` boundaries.
- Enables third-party monitoring tools (pgwatch, Datadog, etc.) to discover
  pg_trickle metrics via the standard stats interface.

**Effort:** Medium — requires implementing a custom stats kind and registering
collectors. Significant but well-bounded work.

---

### MEDIUM: Custom EXPLAIN Options for Extensions

**What:** Extensions can register custom `EXPLAIN` options.

**Opportunity for pg_trickle:**
- Implement `EXPLAIN (PGTRICKLE)` or `EXPLAIN (DELTA)` that shows the
  pg_trickle delta query plan alongside the original query plan.
- Could integrate with `pgtrickle.explain_delta()` to provide the same
  information through standard `EXPLAIN` syntax.

**Effort:** Medium — need to register a custom EXPLAIN option and hook into
the EXPLAIN pipeline.

---

### MEDIUM: Temporal Constraints (WITHOUT OVERLAPS)

**What:** `PRIMARY KEY` and `UNIQUE` constraints can now include a range
column with `WITHOUT OVERLAPS` semantics. Foreign keys support `PERIOD`.

**Opportunity for pg_trickle:**
- Stream tables over temporal data (e.g., reservation systems, SCD Type 2
  tables) can benefit from temporal primary keys in their source tables.
- pg_trickle's CDC triggers must handle temporal PK columns correctly — the
  `pk_hash` computation should include the range column.
- Future: stream tables could define temporal validity themselves, enabling
  "as-of" queries on incrementally maintained temporal aggregates.

**Effort:** Medium — CDC changes for temporal PK detection, plus new E2E
tests.

---

### LOW: UUIDv7

**What:** Built-in `uuidv7()` function generates timestamp-ordered UUIDs.

**Opportunity for pg_trickle:**
- Change buffer tables currently use `bigserial` for `change_id`. UUIDv7
  could provide globally unique, time-ordered identifiers that work across
  distributed setups (multi-database, logical replication targets).
- Not a priority unless multi-database CDC is needed.

**Effort:** Low, but limited benefit for current architecture.

---

### LOW: Improved Hash Join / GROUP BY Performance

**What:** Hash join and GROUP BY memory usage and performance improved.

**Opportunity for pg_trickle:**
- Delta queries heavily use hash joins (delta against current state) and
  GROUP BY (aggregate recomputation). These improvements benefit pg_trickle
  automatically.
- Worth re-benchmarking on PG 18 to quantify.

**Effort:** None — free benefit.

---

### LOW: Logical Replication of Generated Columns

**What:** Generated column values can now be published via logical
replication.

**Opportunity for pg_trickle:**
- In WAL mode, pg_trickle's logical replication decoder can now see
  stored generated column values, making WAL-based CDC complete for tables
  with generated columns.
- Virtual generated columns are still excluded from WAL (no physical storage).

**Effort:** Low — verify WAL decoder handles generated columns in the
change buffer schema.

---

### LOW: extension_control_path

**What:** New GUC `extension_control_path` specifies where to find extension
control files.

**Opportunity for pg_trickle:**
- Simplifies non-standard installation layouts (e.g., NixOS, custom Kubernetes
  init containers).
- Document in INSTALL.md as an alternative to the default `sharedir`.

**Effort:** Documentation only.

---

### LOW: Idle Replication Slot Timeout

**What:** `idle_replication_slot_timeout` automatically invalidates slots that
have been idle too long.

**Opportunity for pg_trickle:**
- pg_trickle creates replication slots for WAL-mode CDC. If the scheduler is
  disabled for extended periods, these slots could be auto-invalidated.
- pg_trickle should monitor slot health and warn before this threshold is
  reached (the `slot_health()` function already does some of this).
- Document the interaction in CONFIGURATION.md.

**Effort:** Low — documentation and a health check enhancement.

---

## PostgreSQL 19 Features — Upcoming

### HIGH: Automatic wal_level=logical Enablement

**What:** When `wal_level = replica`, PostgreSQL 19 can automatically enable
logical replication features when needed. A new GUC `effective_wal_level`
reports the actual level.

**Opportunity for pg_trickle:**
- This is a **game-changer** for pg_trickle's hybrid CDC architecture.
  Currently, WAL-mode CDC requires the DBA to manually set
  `wal_level = logical` and restart. With PG 19, pg_trickle can trigger the
  upgrade automatically.
- The trigger→WAL transition path becomes zero-config: users install
  pg_trickle, create stream tables, and the system automatically transitions
  to WAL-based CDC without any manual configuration.
- pg_trickle should check `effective_wal_level` instead of `wal_level` when
  deciding whether WAL mode is available.
- The `check_cdc_health()` function should report `effective_wal_level`.

**Effort:** Medium — conditional compilation for PG 19+, update WAL detection
logic.

---

### HIGH: SQL/PGQ — Property Graph Queries

**What:** PostgreSQL 19 adds support for SQL Property Graph Queries (SQL/PGQ),
allowing graph pattern matching over regular relational tables.

**Opportunity for pg_trickle:**
- pg_trickle's DAG visualization (`dependency_tree()`, `diamond_groups()`)
  could leverage SQL/PGQ internally to express graph queries over the
  dependency catalog.
- **IVM of graph queries:** If users define stream tables over graph queries,
  pg_trickle needs to support them. Since PG processes SQL/PGQ as views
  (rewritten to standard joins), the view-inlining pass (#0) should handle
  them automatically — but this needs verification.
- **Competitive advantage:** Being the first IVM system to support incremental
  maintenance of graph queries would be significant.

**Effort:** Medium — test view inlining with SQL/PGQ definitions, add E2E
tests.

---

### HIGH: REPACK CONCURRENTLY

**What:** A new `REPACK` command replaces `VACUUM FULL` and `CLUSTER`, with a
`CONCURRENTLY` option that avoids access-exclusive locks.

**Opportunity for pg_trickle:**
- Stream table storage tables grow and accumulate bloat from repeated
  `MERGE`/`DELETE`/`INSERT` operations. Currently, `VACUUM FULL` on a stream
  table requires downtime.
- `REPACK CONCURRENTLY` can reclaim space without blocking reads of stream
  tables — a major operational improvement.
- pg_trickle could offer a `pgtrickle.repack_stream_table()` function that
  coordinates REPACK with the scheduler (suspending refreshes during the
  operation).
- Change buffer tables also benefit from REPACK since they accumulate dead
  tuples from consumed changes.

**Effort:** Medium — add a SQL-callable wrapper function and scheduler
coordination.

---

### HIGH: INSERT ... ON CONFLICT DO SELECT ... RETURNING

**What:** A new conflict handling action `DO SELECT` that returns the
conflicting row, optionally locking it with `FOR UPDATE/SHARE`.

**Opportunity for pg_trickle:**
- The catalog CRUD operations in `src/catalog.rs` use
  `INSERT ... ON CONFLICT DO UPDATE` for upsert semantics. `DO SELECT` is
  more appropriate for "get or create" patterns (e.g., finding an existing
  stream table by name).
- The `create_stream_table_if_not_exists()` function could use this instead
  of a separate SELECT + INSERT.
- The change buffer compaction (`compact_change_buffer()`) could use this for
  deduplication.

**Effort:** Low — targeted catalog query updates.

---

### HIGH: Window Functions IGNORE NULLS / RESPECT NULLS

**What:** `lead()`, `lag()`, `first_value()`, `last_value()`, and
`nth_value()` now support `IGNORE NULLS` and `RESPECT NULLS` options.

**Opportunity for pg_trickle:**
- pg_trickle must parse and handle these new options in the DVM parser's
  window function handling. If a user defines a stream table with
  `first_value(col IGNORE NULLS)`, the parser must preserve this in the
  operator tree and the delta rewrite must respect it.
- The `Window` operator's partition-based recomputation strategy should work
  correctly since it recomputes the full partition, but the SQL generation
  must emit the `IGNORE NULLS` / `RESPECT NULLS` syntax.

**Effort:** Medium — parser changes in `src/dvm/parser/` and window operator
SQL generation.

---

### HIGH: GROUP BY ALL

**What:** `GROUP BY ALL` automatically groups by all non-aggregate,
non-window-function columns in the target list.

**Opportunity for pg_trickle:**
- pg_trickle's parser must handle `GROUP BY ALL` by resolving it to the
  explicit column list before building the operator tree. PostgreSQL's parser
  will likely expand this before pg_trickle sees it, but this needs
  verification.
- The auto-rewrite pipeline should work correctly since it operates on the
  parsed query tree after PostgreSQL's parser has expanded `GROUP BY ALL`.

**Effort:** Low — verify behavior, add E2E test.

---

### HIGH: UPDATE/DELETE FOR PORTION OF (Temporal DML)

**What:** Temporal DML operations that work on a range portion of a temporal
table, automatically splitting or trimming rows.

**Opportunity for pg_trickle:**
- If a source table uses temporal `FOR PORTION OF` DML, the CDC triggers will
  see standard INSERT/UPDATE/DELETE events (PostgreSQL decomposes `FOR PORTION
  OF` into regular DML). pg_trickle should handle this correctly today, but
  needs testing.
- Stream tables over temporal data become more useful when the underlying data
  is maintained with temporal DML.
- Temporal aggregate stream tables (e.g., "total active subscriptions per day
  range") could benefit from temporal-aware delta computation.

**Effort:** Medium — E2E tests for temporal DML as source changes, verify CDC
correctness.

---

### MEDIUM: NOTIFY Optimization — Wake Only Listening Backends

**What:** `NOTIFY` now only wakes backends that are actually listening to the
specified channel.

**Opportunity for pg_trickle:**
- pg_trickle uses `NOTIFY` extensively for refresh alerting and scheduler
  coordination. On busy systems, this optimization reduces wakeup storms.
- Particularly beneficial for the `pooler_compatibility_mode = false` path
  where NOTIFY is active.
- No code changes needed — free benefit.

**Effort:** None — free benefit on PG 19.

---

### MEDIUM: Planner Hooks (planner_setup_hook / planner_shutdown_hook)

**What:** New hooks that fire before and after the planner runs.

**Opportunity for pg_trickle:**
- pg_trickle could use `planner_setup_hook` to inject planner hints for
  delta queries (e.g., force index scans for small deltas, disable
  sequential scans on change buffer lookups).
- This is cleaner than the current approach of using `SET` commands within
  SPI to modify planner behavior.
- Could implement the "P3-4: Index-aware MERGE planning" feature more
  robustly by hooking into the planner rather than setting GUCs.

**Effort:** Medium — requires hooking into the planner via pgrx, which may
need pgrx updates.

---

### MEDIUM: SRF Replacement by Extensions

**What:** Extensions can now replace set-returning functions in the FROM
clause with SQL queries.

**Opportunity for pg_trickle:**
- This could enable pg_trickle to expose stream table contents through a
  virtual SRF that transparently triggers a refresh if the data is stale.
- Could be used to implement "lazy refresh" — `SELECT * FROM pgtrickle.st('my_st')`
  triggers a refresh on demand if the data is older than the schedule.

**Effort:** High — exploratory, needs design.

---

### MEDIUM: Logical Replication Sequence Sync

**What:** Subscriber sequences can now be synchronized with publisher values.

**Opportunity for pg_trickle:**
- Not directly relevant to IVM, but useful for multi-database pg_trickle
  deployments where stream tables are replicated.
- If stream tables use sequences (e.g., for `pgt_id`), replication setups
  need synchronized sequences.

**Effort:** Low — documentation for replication scenarios.

---

### MEDIUM: PUBLICATION EXCEPT Clause

**What:** `CREATE/ALTER PUBLICATION ... ALL TABLES EXCEPT (...)`.

**Opportunity for pg_trickle:**
- When using WAL-mode CDC, pg_trickle creates publications for source tables.
  The `EXCEPT` clause could be used to create a blanket `ALL TABLES`
  publication that excludes pg_trickle's internal tables (change buffers,
  catalog tables).
- Simplifies WAL publication management for environments that use both
  pg_trickle and logical replication for other purposes.

**Effort:** Low — update publication creation logic in `src/cdc.rs`.

---

### LOW: COPY TO FORMAT JSON

**What:** `COPY TO` can output JSON format.

**Opportunity for pg_trickle:**
- The `export_definition()` function could use `COPY ... FORMAT JSON` for
  stream table data export, providing a standard JSON export path.

**Effort:** Low, minimal benefit.

---

### LOW: pg_plan_advice / pg_stash_advice

**What:** New extensions for controlling and stabilizing planner decisions
per query ID.

**Opportunity for pg_trickle:**
- Delta queries can suffer from plan instability (the optimizer may choose
  different plans as change buffer sizes vary). `pg_stash_advice` could pin
  good plans for delta queries.
- pg_trickle could automatically register advice for its generated delta
  SQL via the query ID.

**Effort:** Medium — integration with an optional extension.

---

### LOW: Improved ANTI JOIN Optimization

**What:** More `NOT IN` and `LEFT JOIN` patterns convert to efficient ANTI
JOINs.

**Opportunity for pg_trickle:**
- pg_trickle's `AntiJoin` operator generates `NOT EXISTS` subqueries. The
  improved optimizer should produce better plans for these automatically.
- The delta DELETE queries that use `NOT IN` patterns for identifying rows
  to remove will also benefit.

**Effort:** None — free benefit.

---

### LOW: CHECKPOINT FLUSH_UNLOGGED

**What:** `CHECKPOINT (FLUSH_UNLOGGED)` persists UNLOGGED table data.

**Opportunity for pg_trickle:**
- pg_trickle supports UNLOGGED change buffer tables
  (`convert_buffers_to_unlogged()`). Before a planned restart, users could
  `CHECKPOINT (FLUSH_UNLOGGED)` to persist buffer contents, then safely
  restart without losing buffered changes.
- Document this as an operational best practice.

**Effort:** Documentation only.

---

## Hypothetical / Future Features to Watch

### Core IVM in PostgreSQL

**Status:** Active patch on pgsql-hackers commitfest since 2019, not yet
committed. The `pg_ivm` extension exists as a community alternative.

**Impact on pg_trickle:**
- If PostgreSQL ships core IVM, pg_trickle's immediate differentiator would
  shift from "IVM exists" to "IVM with scheduling, CDC, DAG management,
  monitoring, and broader SQL coverage."
- Core IVM proposals only support immediate maintenance (AFTER triggers with
  transition tables). pg_trickle's deferred/scheduled maintenance, WAL-based
  CDC, and background worker scheduling would remain unique.
- The core IVM patch supports only SPJ + simple aggregates. pg_trickle
  supports 20+ operators including window functions, recursive CTEs, LATERAL,
  set operations, and correlated subqueries.
- **Strategy:** Monitor the commitfest. If core IVM lands, consider
  interoperability (e.g., allow pg_trickle to manage core IVM views as
  sources, or share the counting algorithm infrastructure). Highlight
  pg_trickle's broader SQL coverage and scheduling capabilities as
  differentiators.

---

### Table Access Method (TAM) for Change Buffers

**Status:** Hypothetical — TAM API has existed since PG 12 but custom
implementations are rare.

**Opportunity for pg_trickle:**
- Implement a custom table access method optimized for append-heavy,
  range-scan workloads (exactly what change buffers do).
- A custom TAM could use LSN-ordered storage, skip MVCC overhead for
  consumed changes, and provide zero-copy delta scans.
- This would be a significant performance improvement for high-throughput
  CDC scenarios.

**Effort:** Very high — requires deep PostgreSQL internals knowledge and
extensive testing.

---

### Native LISTEN/NOTIFY Improvements

**Status:** PG 19 already improves targeted wakeups. Future improvements
could include persistent notifications, delivery guarantees, or payload size
increases.

**Opportunity for pg_trickle:**
- Persistent notifications would allow pg_trickle to reliably signal
  external consumers when a stream table is refreshed, even if the consumer
  was temporarily disconnected.
- Larger payloads could carry refresh metadata (row counts, duration) in the
  notification itself.

---

### Parallel DML (INSERT/UPDATE/DELETE/MERGE)

**Status:** Not yet implemented in any PostgreSQL version. Periodic discussion
on pgsql-hackers.

**Opportunity for pg_trickle:**
- Parallel MERGE would dramatically accelerate delta application for large
  change batches. Currently, MERGE is single-threaded and is the bottleneck
  for pg_trickle at high change rates.
- Parallel INSERT would speed up full refresh repopulation.
- pg_trickle could implement its own parallel delta application using multiple
  SPI connections in parallel worker processes — but this is complex and
  risks deadlocks.

**Effort:** N/A (waiting on PostgreSQL) — but worth tracking closely.

---

### Incremental Sort for MERGE

**Status:** Incremental sort exists for SELECT but not for MERGE's internal
sort step.

**Opportunity for pg_trickle:**
- MERGE with a covering index on the target (stream table) could use
  incremental sort on the source (delta) to improve performance when the
  delta is partially ordered by the merge key.
- This is a PostgreSQL optimizer improvement that would benefit pg_trickle
  automatically if implemented.

---

### Column-Store / Hybrid Storage

**Status:** Discussed in PostgreSQL community but no concrete patch.
Third-party extensions (Citus columnar, Hydra) exist.

**Opportunity for pg_trickle:**
- Analytical stream tables (wide tables with many columns, few updates)
  would benefit enormously from columnar storage.
- pg_trickle's selective CDC already reduces column I/O. Columnar storage
  of the stream table itself would complete the picture.
- If PostgreSQL adds a columnar TAM, pg_trickle should support it as a
  storage target.

---

### Pluggable WAL Decoders (In-Process)

**Status:** Hypothetical. Currently, logical decoding requires a replication
connection or `pg_logical_slot_get_changes()`.

**Opportunity for pg_trickle:**
- An in-process WAL decoder hook would eliminate the overhead of the
  replication protocol for WAL-mode CDC.
- pg_trickle's background worker could directly consume WAL records and
  write to change buffers without the serialization/deserialization overhead
  of `pgoutput`.
- This is the ultimate performance optimization for WAL-based CDC.

**Effort:** N/A (hypothetical) — but pg_trickle should advocate for this in
the PostgreSQL community.

---

### Event Trigger for DML

**Status:** PostgreSQL event triggers only fire for DDL events, not DML.
Periodic discussion about extending them.

**Opportunity for pg_trickle:**
- DML event triggers would provide a cleaner CDC mechanism than row-level
  AFTER triggers, with lower per-row overhead.
- Could replace the current trigger-based CDC entirely, with better
  performance characteristics (statement-level DML events with transition
  tables, similar to IMMEDIATE mode but for deferred maintenance).

---

## Impact Matrix

| Feature | Priority | PG Version | Affects | Effort |
|---------|----------|------------|---------|--------|
| OLD/NEW in MERGE RETURNING | HIGH | 18 | Refresh, ST-to-ST | Medium |
| AIO subsystem | HIGH | 18 | Performance | Low (docs) |
| Virtual generated columns | HIGH | 18 | CDC | Medium |
| NOT ENFORCED constraints | HIGH | 18 | Storage, optimizer | Low |
| Auto wal_level=logical | HIGH | 19 | CDC (WAL mode) | Medium |
| SQL/PGQ graph queries | HIGH | 19 | Parser, SQL coverage | Medium |
| REPACK CONCURRENTLY | HIGH | 19 | Operations | Medium |
| ON CONFLICT DO SELECT | HIGH | 19 | Catalog CRUD | Low |
| Window IGNORE NULLS | HIGH | 19 | DVM parser | Medium |
| GROUP BY ALL | HIGH | 19 | Parser | Low |
| Temporal DML | HIGH | 19 | CDC, SQL coverage | Medium |
| Extension stats API | MEDIUM | 18 | Monitoring | Medium |
| Custom EXPLAIN options | MEDIUM | 18 | Diagnostics | Medium |
| Skip scan B-tree | MEDIUM | 18 | Indexing | Low |
| PG_MODULE_MAGIC_EXT | MEDIUM | 18 | Infrastructure | Low |
| Temporal constraints | MEDIUM | 18 | CDC, SQL coverage | Medium |
| NOTIFY optimization | MEDIUM | 19 | Alerting | None |
| Planner hooks | MEDIUM | 19 | Delta planning | Medium |
| SRF replacement | MEDIUM | 19 | Lazy refresh | High |
| PUBLICATION EXCEPT | MEDIUM | 19 | WAL CDC | Low |
| Core IVM (hypothetical) | HIGH | Future | Strategy | — |
| Parallel DML | HIGH | Future | Performance | — |
| Column-store TAM | MEDIUM | Future | Storage | — |
| Pluggable WAL decoder | MEDIUM | Future | CDC | — |

---

*This report should be reviewed when PostgreSQL 19 beta is released
(expected mid-2026) and updated as features are finalized or new ones are
committed.*
