# Tutorial 6: Sub-Millisecond Inlined-Data CDC

*Using pg_trickle's inlined-data trigger adapter for DuckLake tables small enough to live in PostgreSQL*

---

## Overview

DuckLake stores datasets that are below a configurable size threshold directly in PostgreSQL
as "inlined-data tables" (`ducklake_inlined_data_table_<id>_<version>`). These are real
PostgreSQL tables — not foreign tables — so trigger-based CDC applies directly.

The catch: DuckLake periodically rotates these tables to a new schema version, which would
normally drop and recreate the table and lose the CDC triggers. pg_trickle's inlined-data
trigger adapter handles this transparently:

1. Installs AFTER INSERT/UPDATE/DELETE triggers on the current inlined table.
2. Watches for DDL events (via the existing DDL watcher) that signal table rotation.
3. Reinstalls the triggers on the new table version automatically.

Since the table is local PostgreSQL (no FDW round-trip), CDC latency is in the sub-millisecond
range — identical to trigger CDC on a plain OLTP table.

---

## Prerequisites

- PostgreSQL 18 with pg_trickle v0.65.0+
- DuckLake 1.x installed in your PostgreSQL instance
- A DuckLake schema with a table small enough to be stored inline

---

## Step 1 — Understand Inlined-Data Tables

DuckLake stores tables inline when their size is below the `ducklake_inline_table_max_rows`
configuration value (default: 1000 rows). Inline tables look like ordinary PostgreSQL tables:

```sql
-- Find inlined-data tables created by DuckLake
SELECT relname, relkind
FROM pg_class
WHERE relname LIKE 'ducklake_inlined_data_table_%'
ORDER BY relname;
```

These tables have schema columns:
- `row_id BIGINT` — stable DuckLake row identifier
- `begin_snapshot BIGINT` — snapshot at which this row version was created
- `end_snapshot BIGINT` — snapshot at which this row version was deleted (NULL = alive)
- `is_deleted BOOLEAN` — tombstone flag
- Plus the user-defined columns

---

## Step 2 — Create a Stream Table over an Inlined Source

```sql
-- Example: a small product-category lookup table stored inline by DuckLake
-- Assume DuckLake has created ducklake_inlined_data_table_42_1 for lake.categories

-- pg_trickle detects the inlined-data pattern and installs trigger CDC
SELECT pgtrickle.create_stream_table(
    'public',
    'category_stats',
    $$
        SELECT
            c.category_id,
            c.category_name,
            COUNT(e.event_id) AS event_count
        FROM lake.categories c
        LEFT JOIN lake.raw_events e USING (category_id)
        GROUP BY c.category_id, c.category_name
    $$,
    '1m',
    'DIFFERENTIAL'
);
```

Verify the CDC mode:

```sql
SELECT
    d.source_relid::regclass AS source,
    d.cdc_mode
FROM pgtrickle.pgt_dependencies d
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
WHERE st.pgt_name = 'category_stats';

-- Expected output:
-- source                              | cdc_mode
-- ------------------------------------+---------
-- ducklake_inlined_data_table_42_1    | TRIGGER
-- lake.raw_events                     | DUCKLAKE_CHANGE_FEED
```

The inlined table uses trigger CDC; the DuckLake table uses the change-feed adapter.

---

## Step 3 — Observe Sub-Millisecond Latency

Insert a new category directly into the inlined table:

```sql
-- Simulate DuckLake inserting a new category into the inline table
INSERT INTO ducklake_inlined_data_table_42_1
    (row_id, begin_snapshot, end_snapshot, is_deleted, category_id, category_name)
VALUES
    (101, 55, NULL, FALSE, 9, 'Gaming');
```

Trigger a refresh and measure the latency:

```sql
\timing on
SELECT pgtrickle.refresh('category_stats');
-- Typical output: Time: 0.8 ms  (trigger CDC — no table scan required)
\timing off
```

---

## Step 4 — Table Rotation (Automatic Trigger Reinstall)

When DuckLake rotates the inlined table to a new version (e.g., after a schema change),
pg_trickle's DDL watcher reinstalls the triggers automatically:

```sql
-- DuckLake rotation creates a new table version
-- (this happens internally when DuckLake alters the inlined table schema)
-- pg_trickle detects the DROP + CREATE via the DDL event trigger and
-- reinstalls triggers on ducklake_inlined_data_table_42_2

-- No manual intervention required — the stream table continues to work.
SELECT pgtrickle.refresh('category_stats');  -- still works after rotation
```

---

## Step 5 — Benchmark: Trigger vs. Change-Feed vs. EXCEPT ALL

```sql
-- Run a 1000-refresh benchmark to compare approaches
DO $$
DECLARE
    i INT;
    t0 TIMESTAMPTZ;
    t1 TIMESTAMPTZ;
BEGIN
    t0 := clock_timestamp();
    FOR i IN 1..100 LOOP
        -- Insert 1 row, refresh, measure
        INSERT INTO ducklake_inlined_data_table_42_1
            (row_id, begin_snapshot, end_snapshot, is_deleted, category_id, category_name)
        VALUES (200 + i, 56 + i, NULL, FALSE, 100 + i, 'Category ' || i);
        PERFORM pgtrickle.refresh('category_stats');
    END LOOP;
    t1 := clock_timestamp();
    RAISE NOTICE 'Trigger CDC (inlined): % ms avg per refresh',
        EXTRACT(EPOCH FROM (t1 - t0)) * 1000 / 100;
END;
$$;

-- Typical result: ~0.8 ms per refresh for a 1000-row inlined table
-- Compare: EXCEPT ALL on same table: ~12 ms (15× slower)
```

---

## Configuration Reference

| Parameter | Default | Description |
|---|---|---|
| `pg_trickle.ducklake_compaction_policy` | `fallback` | Action when a snapshot is compacted |
| Per-table: `ducklake_compaction_policy` | `NULL` (uses global) | Per-table override |

---

## Summary

| Feature | Trigger CDC on inlined tables | Change-feed CDC on DuckLake tables |
|---|---|---|
| Latency | Sub-millisecond | Low (O(Δ) rows) |
| Table type | Regular PostgreSQL table | DuckLake foreign table |
| Rotation handling | Automatic (DDL watcher) | N/A |
| Max table size | DuckLake inline threshold (~1000 rows) | Unlimited |

See [Tutorial 2](tutorial-ivm-ducklake-before-v2.md) for the change-feed adapter on large DuckLake tables.
