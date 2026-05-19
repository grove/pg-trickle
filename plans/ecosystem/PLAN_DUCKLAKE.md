# pg_trickle × DuckLake Integration Plan

Date: 2026-05-19
Status: PROPOSED

---

## Executive Summary

DuckLake is an open lakehouse format (v1.0 released April 2026) from the DuckDB
team that stores data in Parquet files and manages all metadata in a standard SQL
database — most commonly **PostgreSQL**. This architectural choice creates a
uniquely powerful synergy with pg_trickle: both systems operate inside the same
PostgreSQL instance, share the same transactional guarantees, and complement each
other's strengths.

pg_trickle provides world-class incremental view maintenance (IVM) inside
PostgreSQL. DuckLake provides scalable lakehouse storage and multi-engine
analytics. Together they can deliver **incrementally maintained materialized views
over data lake tables** — something no existing system offers today.

DuckLake's own roadmap explicitly lists "Incremental materialized views" as a
v2.0 goal. pg_trickle is uniquely positioned to deliver this capability.

---

## Table of Contents

- [Background: What Is DuckLake?](#background-what-is-ducklake)
- [Architectural Synergies](#architectural-synergies)
- [Integration Opportunities](#integration-opportunities)
  - [INT-1: DuckLake as Stream Table Source (Foreign Table Path)](#int-1-ducklake-as-stream-table-source-foreign-table-path)
  - [INT-2: DuckLake Inlined Data as Native CDC Source](#int-2-ducklake-inlined-data-as-native-cdc-source)
  - [INT-3: DuckLake Change Feed Polling](#int-3-ducklake-change-feed-polling)
  - [INT-4: Stream Table Results Exported to DuckLake](#int-4-stream-table-results-exported-to-ducklake)
  - [INT-5: IVM Engine for DuckLake (v2.0 Collaboration)](#int-5-ivm-engine-for-ducklake-v20-collaboration)
  - [INT-6: DuckLake Metadata Monitoring via Stream Tables](#int-6-ducklake-metadata-monitoring-via-stream-tables)
  - [INT-7: Hybrid OLTP/OLAP Pipeline](#int-7-hybrid-oltpolap-pipeline)
- [Feature Ideas for pg_trickle](#feature-ideas-for-pg_trickle)
- [Blog Post & Tutorial Ideas](#blog-post--tutorial-ideas)
- [Implementation Roadmap](#implementation-roadmap)
- [Risks & Considerations](#risks--considerations)

---

## Background: What Is DuckLake?

DuckLake is a lakehouse format that:

1. **Stores data** in immutable Parquet files (local disk, S3, GCS, Azure Blob).
2. **Stores metadata** (schemas, snapshots, statistics, file locations) in a
   standard SQL database (PostgreSQL, SQLite, or DuckDB).
3. **Provides ACID transactions** over multi-table operations using the catalog
   database's transactional guarantees.
4. **Supports time travel** via lightweight snapshots (just a few rows in the
   metadata database per snapshot).
5. **Provides a Data Change Feed** via `table_changes(table, start_snapshot,
   end_snapshot)` returning insert/delete/update_preimage/update_postimage rows.
6. **Supports data inlining** — small changes (≤10 rows by default) are stored
   directly in PostgreSQL tables within the catalog database, avoiding small
   Parquet file proliferation.
7. **Has multiple clients** — DuckDB (reference), Apache DataFusion, Apache
   Spark, Trino, Pandas, and more.

**Key architecture diagram:**
```
┌──────────────────────────────────────────────────────┐
│              DuckDB / Spark / Trino / ...             │  ← Compute
├──────────────────────────────────────────────────────┤
│                    DuckLake Format                    │
├──────────────┬───────────────────────────────────────┤
│  PostgreSQL  │          Object Storage (S3)          │
│  (Catalog)   │          (Parquet files)              │
│  28 tables   │                                       │
└──────────────┴───────────────────────────────────────┘
```

When PostgreSQL is the catalog, the DuckLake metadata tables (prefixed
`ducklake_`) live in a dedicated database or schema alongside other PostgreSQL
data. pg_trickle runs in the same PostgreSQL instance and can see these tables.

---

## Architectural Synergies

| Dimension | DuckLake | pg_trickle | Synergy |
|-----------|----------|------------|---------|
| **Runs inside PostgreSQL** | Catalog lives in PG | Extension runs in PG | Same process, zero-copy metadata access |
| **Change tracking** | `table_changes()` provides insert/delete/update deltas between snapshots | CDC (triggers/WAL) captures row-level changes | DuckLake's change feed is a natural delta source for pg_trickle |
| **Data inlining** | Small changes stored in PG tables (`ducklake_inlined_data_table_*`) | Triggers can watch any PG table | pg_trickle can track inlined DuckLake data natively |
| **Incremental materialized views** | On roadmap for v2.0, not yet implemented | Core competency, production-ready | pg_trickle can *be* the IVM engine for DuckLake |
| **Snapshot semantics** | Lightweight snapshots, millions supported | Frontier-based version tracking | Snapshot IDs map naturally to frontier positions |
| **Multi-engine reads** | DuckDB, Spark, Trino, DataFusion | Exposes results as PostgreSQL tables | Stream table results queryable by all DuckLake clients via `postgres_scanner` |
| **Statistics** | Table/column/file-level stats in catalog | Uses PG stats for planning | DuckLake stats can guide pg_trickle cost model |
| **ACID** | Via PG transactions | Via PG transactions | Both participate in same transaction boundaries |

---

## Integration Opportunities

### INT-1: DuckLake as Stream Table Source (Foreign Table Path)

**What:** Use DuckDB's `postgres_scanner` in reverse — expose DuckLake tables as
PostgreSQL foreign tables via `duckdb_fdw` or `parquet_fdw`, then create
pg_trickle stream tables over them.

**How it works today:**
```sql
-- 1. Install parquet_fdw (or duckdb_fdw)
CREATE EXTENSION parquet_fdw;
CREATE SERVER duckdb_server FOREIGN DATA WRAPPER parquet_fdw;

-- 2. Create foreign table pointing to DuckLake Parquet files
CREATE FOREIGN TABLE events (
    user_id   INT,
    event     TEXT,
    timestamp TIMESTAMPTZ
) SERVER duckdb_server
  OPTIONS (filename 's3://my-lake/data_files/*.parquet');

-- 3. Enable polling and create stream table
SET pg_trickle.foreign_table_polling = on;
SELECT pgtrickle.create_stream_table(
    name  => 'events_per_user',
    query => $$
        SELECT user_id, COUNT(*) as event_count
        FROM events
        GROUP BY user_id
    $$,
    schedule     => '30s',
    refresh_mode => 'DIFFERENTIAL'
);
```

**Status:** Already works today with `pg_trickle.foreign_table_polling = on`.
The polling mechanism does a full scan + EXCEPT ALL diff each cycle.

**Improvement opportunity:** Instead of full-scan polling, use DuckLake's
`table_changes()` function to obtain only the delta. See INT-3.

**Effort:** Low (documentation/blog post for existing functionality)

---

### INT-2: DuckLake Inlined Data as Native CDC Source

**What:** When DuckLake uses PostgreSQL as its catalog, small writes are stored
directly in PostgreSQL tables (`ducklake_inlined_data_table_<id>_<version>`).
pg_trickle can place standard AFTER triggers on these inlined data tables to
capture changes with zero additional cost.

**How:**
```sql
-- DuckLake inlined data tables are regular PG tables with columns:
--   row_id BIGINT, begin_snapshot BIGINT, end_snapshot BIGINT, <data columns...>

-- pg_trickle could track inserts to these tables:
-- When begin_snapshot is set → row inserted
-- When end_snapshot is set  → row deleted

-- Stream table over inlined DuckLake data:
SELECT pgtrickle.create_stream_table(
    name  => 'active_lake_orders',
    query => $$
        SELECT order_id, customer_id, total
        FROM ducklake_inlined_data_table_5_1
        WHERE end_snapshot IS NULL  -- only live rows
    $$,
    schedule => 'immediate'  -- real-time as DuckLake commits
);
```

**Key insight:** Inlined data tables are standard PostgreSQL heap tables.
pg_trickle's trigger-based CDC works on them without modification. This gives
**sub-millisecond latency** on DuckLake changes (as long as they're inlined).

**Limitations:**
- Only works for small changes (≤ `data_inlining_row_limit` rows)
- Schema changes create new inlined tables (need to track DDL)
- DuckLake's `CHECKPOINT`/flush moves inlined data to Parquet (stops tracking)

**Effort:** Medium (need CDC adapter for inlined table schema conventions)

---

### INT-3: DuckLake Change Feed Polling

**What:** Build a dedicated CDC polling adapter that calls DuckLake's
`table_changes()` function to retrieve deltas between snapshots, converting them
into pg_trickle's change buffer format.

**Architecture:**
```
DuckLake table_changes(tbl, last_snapshot, current_snapshot)
    │
    ▼
┌─────────────────────────────────┐
│  DuckLake Change Feed Adapter   │  ← New component
│  (background worker)            │
│                                 │
│  • Tracks last-consumed         │
│    snapshot per DuckLake table   │
│  • Calls table_changes() via    │
│    DuckDB's postgres interface   │
│  • Writes deltas to             │
│    pgtrickle_changes.changes_*   │
└─────────────────────────────────┘
    │
    ▼
pg_trickle DVM engine (standard differential refresh path)
```

**DuckLake's `table_changes()` output:**
```
snapshot_id | rowid | change_type        | col1 | col2 | ...
-----------+-------+--------------------+------+------+----
3          | 0     | insert             | 42   | foo  |
4          | 1     | update_preimage    | 43   | bar  |
4          | 1     | update_postimage   | 43   | baz  |
5          | 2     | delete             | 44   | qux  |
```

This maps directly to pg_trickle's change buffer format (I/U/D actions with
old/new columns). The adapter would:

1. Connect to DuckDB (via `duckdb_fdw` or a background worker with embedded DuckDB)
2. Track the last-consumed snapshot ID in `pgt_change_tracking`
3. Periodically call `table_changes(tbl, last_snapshot, current_snapshot())`
4. Transform results into change buffer rows
5. Write to `pgtrickle_changes.changes_<foreign_table_oid>`

**Advantages over full-scan polling:**
- O(delta) instead of O(full table) per refresh cycle
- DuckLake provides pre-computed change types (insert/delete/update)
- Snapshot IDs give a clean resumption point (no LSN needed)
- Works for data stored in Parquet on S3 (not just inlined data)

**Effort:** High (new adapter component, DuckDB connectivity)

---

### INT-4: Stream Table Results Exported to DuckLake

**What:** Instead of (or in addition to) storing stream table results in a
PostgreSQL heap table, export the delta to a DuckLake-managed table. This
makes incrementally maintained results available to all DuckLake-compatible
analytics engines (DuckDB, Spark, Trino, DataFusion).

**Architecture:**
```
Source Tables (PG)
    │  CDC triggers
    ▼
pg_trickle DVM Engine
    │  computes delta
    ▼
┌─────────────────────────────────┐
│  DuckLake Sink Adapter          │  ← New component
│                                 │
│  • Receives INSERT/DELETE delta  │
│  • Writes new Parquet file to   │
│    object storage                │
│  • Commits new snapshot to      │
│    DuckLake catalog (PG tables)  │
└─────────────────────────────────┘
    │
    ▼
DuckLake (Parquet on S3 + PG catalog)
    │
    ▼
DuckDB / Spark / Trino consumers
```

**API sketch:**
```sql
SELECT pgtrickle.create_stream_table(
    name     => 'revenue_by_region',
    query    => $$
        SELECT region, SUM(amount) as total
        FROM orders JOIN customers ON ...
        GROUP BY region
    $$,
    schedule => '5s',
    -- New: sink results to DuckLake instead of (or alongside) PG table
    sink     => 'ducklake',
    sink_options => '{
        "catalog_db": "ducklake_catalog",
        "data_path": "s3://analytics-lake/stream_tables/",
        "table_name": "revenue_by_region"
    }'
);
```

**Key value proposition:** "Compute IVM in PostgreSQL, serve results at data lake
scale." Users get sub-second latency on aggregations while results are
automatically available as a DuckLake table queryable by any analytics engine.

**DuckLake snapshot integration:** Each pg_trickle refresh cycle creates a new
DuckLake snapshot. Consumers can use `table_changes()` on the stream table's
DuckLake output to see what changed in each refresh cycle — enabling downstream
incremental processing.

**Effort:** Very High (needs Parquet writer, S3 client, DuckLake catalog SQL
transaction logic)

---

### INT-5: IVM Engine for DuckLake (v2.0 Collaboration)

**What:** DuckLake's roadmap explicitly states "Incremental materialized views"
as a v2.0 goal. pg_trickle could provide the IVM engine that makes this
possible, either as a PostgreSQL-side component or by contributing IVM algorithms
to the DuckDB ecosystem.

**Collaboration models:**

#### Option A: PostgreSQL-side IVM (pg_trickle as DuckLake's IVM backend)

```
DuckDB client
    │  CREATE MATERIALIZED VIEW ... WITH INCREMENTAL MAINTENANCE
    ▼
DuckLake catalog (PG) ──▶ pg_trickle extension
    │                          │
    │  data files (Parquet)    │  monitors ducklake_snapshot_changes
    │                          │  computes delta via DVM engine
    ▼                          │  writes result to DuckLake
Object Storage                 │
    ▲                          │
    └──────────────────────────┘
```

When DuckDB writes data to a DuckLake table, pg_trickle (running in the same
PostgreSQL catalog database) detects the snapshot change, reads the delta, applies
the DVM operator tree, and writes the result as a new DuckLake snapshot.

**This is architecturally elegant because:**
- DuckLake already commits snapshots as PG transactions
- pg_trickle already watches PG tables for changes
- Both use the same PostgreSQL instance as their coordination point
- pg_trickle's background worker can react to DuckLake commits

#### Option B: Contribute IVM algorithms to DuckDB extension ecosystem

Implement pg_trickle's differential dataflow algorithms as a standalone DuckDB
extension. This would be a separate project but could share the same IVM theory
and operator tree design.

**Trade-offs:**

| | Option A (PG-side) | Option B (DuckDB extension) |
|---|---|---|
| Leverages existing pg_trickle code | ✅ Directly | ❌ Rewrite in C++ |
| Works with PG catalog DuckLakes | ✅ Native | ✅ Via extension |
| Works without PostgreSQL | ❌ | ✅ |
| Performance for large datasets | Moderate (PG) | High (DuckDB columnar) |
| Time to market | 3–6 months | 12+ months |

**Recommendation:** Pursue Option A first as a proof of concept, then evaluate
whether the DuckDB community wants a native extension.

**Effort:** Very High (deep integration, specification alignment)

---

### INT-6: DuckLake Metadata Monitoring via Stream Tables

**What:** DuckLake's 28 metadata tables live in PostgreSQL. pg_trickle can
create stream tables over them to provide real-time operational dashboards.

**Examples:**
```sql
-- Real-time snapshot rate monitoring
SELECT pgtrickle.create_stream_table(
    name  => 'ducklake_snapshot_rate',
    query => $$
        SELECT date_trunc('minute', snapshot_time) as minute,
               COUNT(*) as snapshots_per_minute,
               COUNT(DISTINCT table_id) as tables_changed
        FROM ducklake_snapshot
        GROUP BY 1
    $$,
    schedule => '10s'
);

-- File growth monitoring
SELECT pgtrickle.create_stream_table(
    name  => 'ducklake_storage_growth',
    query => $$
        SELECT t.table_name,
               COUNT(*) as file_count,
               SUM(ds.file_size_bytes) as total_bytes,
               MAX(ds.snapshot_id) as latest_snapshot
        FROM ducklake_data_file ds
        JOIN ducklake_table t ON t.table_id = ds.table_id
        WHERE ds.deletion_snapshot_id IS NULL
        GROUP BY t.table_name
    $$,
    schedule => '30s'
);

-- Compaction candidates (tables with many small files)
SELECT pgtrickle.create_stream_table(
    name  => 'ducklake_compaction_candidates',
    query => $$
        SELECT t.table_name,
               COUNT(*) as active_files,
               AVG(ds.row_count) as avg_rows_per_file,
               SUM(ds.file_size_bytes) as total_size
        FROM ducklake_data_file ds
        JOIN ducklake_table t ON t.table_id = ds.table_id
        WHERE ds.deletion_snapshot_id IS NULL
        GROUP BY t.table_name
        HAVING COUNT(*) > 100
           AND AVG(ds.row_count) < 10000
    $$,
    schedule => '5m'
);
```

**Value:** Real-time observability on DuckLake operations without external
monitoring infrastructure. Especially useful for multi-tenant DuckLake
deployments where many DuckDB clients are writing concurrently.

**Effort:** Low (just documentation/examples — already works today)

---

### INT-7: Hybrid OLTP/OLAP Pipeline

**What:** A reference architecture combining pg_trickle and DuckLake for the
common pattern: "OLTP writes to PostgreSQL, analytics runs in DuckDB."

**Architecture:**
```
┌─────────────────────────────────────────────────────────────────┐
│                        PostgreSQL                                │
│                                                                 │
│  ┌──────────────┐     ┌──────────────┐     ┌────────────────┐  │
│  │  OLTP Tables │────▶│  pg_trickle  │────▶│ Stream Tables  │  │
│  │  (orders,    │ CDC │  DVM Engine  │delta│ (aggregations, │  │
│  │   products)  │     │              │     │  denormalized)  │  │
│  └──────────────┘     └──────────────┘     └───────┬────────┘  │
│                                                     │           │
│  ┌──────────────────────────────────────────────────┼───────┐   │
│  │            DuckLake Catalog (PG tables)          │       │   │
│  │  ducklake_snapshot, ducklake_data_file, ...      │       │   │
│  └──────────────────────────────────────────────────┼───────┘   │
└─────────────────────────────────────────────────────┼───────────┘
                                                      │
                              ┌────────────────────────┘
                              │  Export delta as Parquet
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    Object Storage (S3)                           │
│  s3://lake/stream_tables/revenue_by_region/                     │
│    ├── data_files/ducklake-abc123.parquet                       │
│    └── data_files/ducklake-def456.parquet                       │
└─────────────────────────────────────────────────────────────────┘
                              │
              ┌───────────────┼───────────────┐
              ▼               ▼               ▼
         ┌─────────┐    ┌─────────┐    ┌─────────┐
         │ DuckDB  │    │  Spark  │    │  Trino  │
         │ (local) │    │(cluster)│    │  (BI)   │
         └─────────┘    └─────────┘    └─────────┘
```

**User story:** "I have an OLTP PostgreSQL database. I want real-time aggregated
views that are also queryable from my data lake by BI tools like Metabase,
Superset, or dbt running on DuckDB."

**This is the killer use case:** pg_trickle handles the hard part (incremental
computation) while DuckLake handles the distribution part (making results
available everywhere in an open format).

**Effort:** Medium–High (combines INT-4 with documentation/examples)

---

## Feature Ideas for pg_trickle

Based on this analysis, specific features to add to pg_trickle:

### F-1: DuckLake-Aware Polling Adapter

Replace the generic EXCEPT ALL polling for DuckLake foreign tables with a
snapshot-aware adapter:

```rust
// New CDC mode for DuckLake sources
enum CdcMode {
    Trigger,
    Wal,
    Polling,         // existing: full scan + EXCEPT ALL
    DuckLakeChangeFeed,  // new: uses table_changes()
}
```

Track `last_consumed_snapshot_id` instead of LSN for DuckLake sources.

### F-2: DuckLake Sink Output Mode

Add a new `sink` parameter to `create_stream_table` that writes results to
DuckLake format instead of (or in addition to) a PostgreSQL heap table:

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'events_hourly',
    query        => $$ ... $$,
    schedule     => '10s',
    sink         => 'ducklake',
    sink_options => '{"catalog_db": "lake", "data_path": "s3://..."}'
);
```

### F-3: Snapshot-Based Frontier

Add a frontier type that uses DuckLake snapshot IDs rather than WAL LSNs:

```json
{
    "ducklake:lake.events": {"snapshot_id": 42},
    "ducklake:lake.users":  {"snapshot_id": 38}
}
```

### F-4: Parquet Delta Export

For any stream table, allow exporting the computed delta as a Parquet file on
each refresh cycle. This is a building block for INT-4:

```sql
SELECT pgtrickle.alter_stream_table(
    'revenue_by_region',
    delta_export_path => 's3://lake/deltas/revenue_by_region/'
);
```

### F-5: DuckLake Inlined Table Trigger Adapter

Specialized trigger function that understands DuckLake's inlined data table
conventions (`begin_snapshot`/`end_snapshot` columns) and translates them into
standard INSERT/DELETE change buffer rows.

---

## Blog Post & Tutorial Ideas

### Tutorial 1: "Real-Time Dashboards on Your Data Lake with pg_trickle + DuckLake"

**Audience:** Data engineers using DuckLake who want fresh aggregations.

**Content:**
1. Set up PostgreSQL with DuckLake catalog + pg_trickle
2. DuckDB clients write events to DuckLake
3. pg_trickle monitors DuckLake metadata tables
4. Create stream tables for dashboard metrics (events/minute, top users, etc.)
5. Connect Grafana to PostgreSQL stream tables
6. Show sub-second refresh latency on metrics derived from lake data

### Tutorial 2: "Incremental Materialized Views for DuckLake (Before v2.0)"

**Audience:** DuckLake users who can't wait for v2.0's native IVM.

**Content:**
1. Explain DuckLake's current lack of materialized views
2. Show how pg_trickle in the catalog PostgreSQL fills this gap
3. End-to-end example: track DuckLake table changes → pg_trickle stream table →
   results queryable via DuckDB's `postgres_scanner`
4. Performance comparison: full refresh vs. pg_trickle differential

### Tutorial 3: "The Modern Data Stack in One Box: PostgreSQL + pg_trickle + DuckLake"

**Audience:** Startups/SMBs wanting a complete analytics stack without Kafka,
Spark, Airflow, etc.

**Content:**
1. OLTP in PostgreSQL (orders, users, products)
2. pg_trickle for real-time aggregations (revenue, funnels, cohorts)
3. DuckLake for historical analytics and time travel
4. DuckDB for ad-hoc queries over lake data
5. All in one PostgreSQL instance + S3 bucket — no orchestration

### Tutorial 4: "Streaming PostgreSQL Changes to Your Data Lake"

**Audience:** Engineers wanting CDC-to-lake without Debezium/Kafka.

**Content:**
1. pg_trickle captures changes to OLTP tables
2. Stream table computes denormalized/aggregated view
3. Delta export writes each refresh's changes as Parquet to S3
4. DuckLake catalogs the exported Parquet files
5. Compare with Debezium → Kafka → Flink → Iceberg pipeline (10x simpler)

### Tutorial 5: "Monitoring Your DuckLake with pg_trickle"

**Audience:** DuckLake operators managing multi-tenant data lakes.

**Content:**
1. Create stream tables over DuckLake metadata tables
2. Real-time alerts: too many small files, snapshot rate spikes, storage growth
3. Grafana dashboard with pg_trickle-powered metrics
4. Automated compaction triggers based on stream table thresholds

### Blog Post: "Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM"

**Audience:** Data infrastructure community (Hacker News, /r/dataengineering).

**Content:**
- Problem: Lakehouse formats (Iceberg, Delta, DuckLake) don't do IVM
- Existing "solutions": Materialize, Flink, RisingWave — all separate systems
- Our approach: IVM inside the catalog database that DuckLake already requires
- Zero additional infrastructure
- Performance benchmarks

### Blog Post: "DuckLake's table_changes() Meets pg_trickle's DVM Engine"

**Audience:** Technical readers interested in incremental computation.

**Content:**
- Deep dive into DuckLake's change feed format
- How it maps to pg_trickle's change buffer model
- Differential refresh on data lake tables without full scans
- Benchmarks: 1M-row DuckLake table, 100-row delta → refresh in microseconds

---

## Implementation Roadmap

### Phase 1: Documentation & Awareness (v0.63)

| Item | Type | Effort |
|------|------|--------|
| Blog post: INT-1 (DuckLake as foreign table source) | Tutorial | 1 day |
| Blog post: INT-6 (monitoring DuckLake metadata) | Tutorial | 1 day |
| Add DuckLake examples to foreign-table-sources.md | Docs | 0.5 day |
| Blog post: "One-box data stack" tutorial | Tutorial | 2 days |

### Phase 2: DuckLake-Optimized Polling (v0.64–v0.65)

| Item | Type | Effort |
|------|------|--------|
| F-1: DuckLake change feed adapter (prototype) | Feature | 1 week |
| F-3: Snapshot-based frontier for DuckLake sources | Feature | 3 days |
| F-5: Inlined data table trigger adapter | Feature | 3 days |
| Integration tests with DuckDB + ducklake extension | Tests | 2 days |
| Tutorial 2: "Incremental MVs for DuckLake" | Tutorial | 2 days |

### Phase 3: DuckLake Sink (v0.66–v0.68)

| Item | Type | Effort |
|------|------|--------|
| F-4: Parquet delta export (using `arrow2` or `parquet` crate) | Feature | 2 weeks |
| F-2: DuckLake sink mode for stream tables | Feature | 2 weeks |
| S3 upload integration | Feature | 1 week |
| DuckLake catalog transaction writer | Feature | 1 week |
| Tutorial 4: "Streaming PG to data lake" | Tutorial | 2 days |
| E2E tests with DuckLake sink | Tests | 1 week |

### Phase 4: DuckLake v2.0 IVM Partnership (v0.70+)

| Item | Type | Effort |
|------|------|--------|
| INT-5 Option A: Full DuckLake IVM backend | Feature | 2–3 months |
| Engage DuckDB team for specification alignment | Community | Ongoing |
| Benchmark suite: IVM over DuckLake at scale | Perf | 2 weeks |
| Joint announcement / blog post | Community | 1 week |

---

## Risks & Considerations

### Technical Risks

1. **DuckDB connectivity from PostgreSQL:** Calling DuckLake's `table_changes()`
   requires DuckDB execution. Options:
   - `duckdb_fdw` extension (exists but maturity varies)
   - Embedded DuckDB in a background worker (complex but possible via C API)
   - External sidecar process that bridges the gap
   - Simply query the DuckLake catalog tables directly (they're in PG)

2. **Parquet writing from Rust/PostgreSQL:** INT-4 requires writing Parquet
   files. The `parquet` crate (part of `arrow-rs`) is mature and can be used in
   a pgrx extension. S3 upload via `aws-sdk-rust` or `opendal`.

3. **DuckLake schema evolution:** When DuckLake tables have their schema altered,
   inlined data tables change. The trigger adapter needs to handle this.

4. **Consistency across boundaries:** If pg_trickle reads DuckLake changes and
   writes results back to DuckLake, there's a potential for circular
   dependencies. Must ensure clear DAG boundaries.

### Strategic Risks

1. **DuckLake v2.0 timeline uncertainty:** If DuckLake ships native IVM before we
   deliver INT-5, our window closes. Mitigated by delivering Phase 1–3 first
   (useful regardless of DuckLake's native IVM).

2. **Competition with Materialize/RisingWave:** These offer streaming over change
   feeds too, but require separate infrastructure. Our advantage is
   co-location with the catalog database.

3. **DuckDB team's openness to collaboration:** INT-5 Option A requires the
   DuckDB team to endorse pg_trickle as an IVM provider. Early community
   engagement is essential.

### Mitigation Strategy

- **Start with zero-dependency integrations** (Phase 1): Blog posts and
  documentation showing what already works require no code changes and no
  external approval.
- **Build incrementally:** Each phase delivers standalone value. Phase 3 doesn't
  depend on Phase 4.
- **Maintain the "SQL is the API" principle:** All integrations work through
  standard SQL interfaces, minimizing coupling to DuckLake internals.

---

## Competitive Landscape

| Solution | Approach | vs pg_trickle + DuckLake |
|----------|----------|--------------------------|
| **Materialize** | Streaming SQL engine | Separate system, no lakehouse, expensive |
| **RisingWave** | Streaming SQL engine | Separate system, needs Kafka/Pulsar |
| **Flink SQL** | Stream processor | Massive infra, JVM, separate cluster |
| **dbt incremental** | Batch incremental | Not true IVM, full scan on schedule |
| **Iceberg + Flink** | Lakehouse + stream | 5+ systems to operate |
| **pg_trickle + DuckLake** | IVM in catalog DB | Zero additional infra, same PG instance |

**Our unique positioning:** The only solution that provides true differential
IVM *inside* the lakehouse catalog database, requiring zero additional
infrastructure beyond PostgreSQL + S3.

---

## Summary

The pg_trickle × DuckLake integration represents a strategic opportunity to:

1. **Immediately** (Phase 1): Publish tutorials showing pg_trickle working with
   DuckLake foreign tables and monitoring DuckLake metadata — establishing
   thought leadership.

2. **Near-term** (Phase 2): Build DuckLake-optimized CDC adapters that
   outperform naive polling by orders of magnitude.

3. **Medium-term** (Phase 3): Enable stream table results to flow into DuckLake,
   making pg_trickle the "real-time computation layer" for data lakes.

4. **Long-term** (Phase 4): Position pg_trickle as the IVM engine for DuckLake
   v2.0, potentially becoming a standard component of every PostgreSQL-backed
   DuckLake deployment.

The fundamental insight is simple: **DuckLake chose PostgreSQL as its catalog
database. pg_trickle lives in PostgreSQL. This co-location is a superpower.**
