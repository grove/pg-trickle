# Tutorial 2: IVM for DuckLake Before v2.0

*How pg_trickle's change-feed adapter delivers O(Δ) differential refresh on DuckLake 1.x tables*

---

## Overview

DuckLake 1.x ships the `table_changes(table, from_snapshot, to_snapshot)` function — a precise
change-data-capture API that returns only the rows that changed between two snapshot IDs. Before
v0.65.0, pg_trickle used a generic `EXCEPT ALL` scan to detect changes on DuckLake-backed foreign
tables. That approach does O(N) work per refresh, regardless of how few rows changed.

This tutorial shows you the before-and-after picture:

| Approach | Work per refresh | When to use |
|---|---|---|
| `EXCEPT ALL` polling (≤ v0.64.0) | O(N) — full table scan | DuckLake tables without `table_changes` |
| Change-feed adapter (v0.65.0+) | O(Δ) — only changed rows | DuckLake 1.x with `table_changes` |

---

## Prerequisites

- PostgreSQL 18 with pg_trickle v0.65.0+
- DuckLake 1.x installed in your PostgreSQL instance
- A DuckLake schema with at least one table

---

## Step 1 — Create a DuckLake Source Table

```sql
-- Initialise a DuckLake catalog (adjust path to your DuckDB file)
SELECT ducklake_init('/var/lib/ducklake/events.db');

-- Create a DuckLake foreign table via DuckLake DDL
-- (executed in DuckDB, visible in PostgreSQL via the FDW)
CREATE TABLE lake.raw_events (
    event_id   BIGINT PRIMARY KEY,
    event_type TEXT   NOT NULL,
    user_id    BIGINT NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    payload    JSONB
);
```

The DuckLake FDW exposes this as a foreign table in PostgreSQL:

```sql
-- Verify the table is visible as a foreign table
SELECT relname, relkind
FROM pg_class
WHERE relname = 'raw_events' AND relkind = 'f';
```

---

## Step 2 — Create a Stream Table (Automatic Change-Feed Detection)

```sql
-- pg_trickle detects that raw_events is backed by a DuckLake FDW
-- and automatically selects the change-feed adapter (CdcMode::DuckLakeChangeFeed).
SELECT pgtrickle.create_stream_table(
    'public',
    'event_summary',
    $$
        SELECT
            date_trunc('hour', occurred_at)  AS hour,
            event_type,
            COUNT(*)                          AS event_count,
            COUNT(DISTINCT user_id)           AS unique_users
        FROM lake.raw_events
        GROUP BY 1, 2
    $$,
    '5m',
    'DIFFERENTIAL'
);
```

Check which CDC mode was selected:

```sql
SELECT
    d.source_relid::regclass AS source,
    d.cdc_mode
FROM pgtrickle.pgt_dependencies d
JOIN pgtrickle.pgt_stream_tables st ON st.pgt_id = d.pgt_id
WHERE st.pgt_name = 'event_summary';

-- Expected output:
-- source          | cdc_mode
-- ----------------+----------------------
-- lake.raw_events | DUCKLAKE_CHANGE_FEED
```

---

## Step 3 — Observe O(Δ) Refresh Behaviour

Insert a batch of events into DuckLake (100 rows out of 10 million):

```sql
-- Simulate a DuckLake batch load (in DuckDB)
INSERT INTO lake.raw_events
SELECT
    generate_series          AS event_id,
    'click'                  AS event_type,
    (random() * 1000)::bigint AS user_id,
    now() - (random() * interval '1 day') AS occurred_at,
    '{}'::jsonb              AS payload
FROM generate_series(10_000_001, 10_000_100);  -- 100 new rows
```

Trigger a manual refresh and observe the row counts:

```sql
SELECT pgtrickle.refresh('event_summary');

SELECT
    action,
    rows_inserted,
    rows_deleted
FROM pgtrickle.pgt_refresh_history
WHERE pgt_id = (
    SELECT pgt_id FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'event_summary'
)
ORDER BY refresh_id DESC
LIMIT 1;

-- Expected output (only the 100 new rows were processed):
-- action       | rows_inserted | rows_deleted
-- DIFFERENTIAL |           100 |            0
```

Compare this with the `EXCEPT ALL` approach, which would scan all 10 million rows.

---

## Step 4 — Configure the Compaction Policy

DuckLake periodically compacts old snapshots. If pg_trickle's frontier references a snapshot
that has been compacted away, the next refresh would fail. Configure how pg_trickle responds:

```sql
-- Global policy: fall back to full refresh on compaction (default)
ALTER SYSTEM SET pg_trickle.ducklake_compaction_policy = 'fallback';

-- Per-table override: raise an error instead (for production tables
-- where silent data re-scans are unacceptable)
ALTER STREAM TABLE event_summary SET (ducklake_compaction_policy = 'error');
```

---

## Step 5 — Inspect the Snapshot Frontier

```sql
SELECT
    frontier -> 'sources' AS ducklake_sources,
    frontier -> 'data_timestamp' AS data_timestamp
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'event_summary';

-- Example output:
-- {
--   "ducklake:lake.raw_events": {
--     "lsn": "0/0",
--     "snapshot_ts": "",
--     "snapshot_id": 42
--   }
-- }
```

The `snapshot_id` advances monotonically with each successful refresh. The next refresh will
call `table_changes(lake.raw_events, 42, <latest_snapshot_id>)` to fetch only the delta.

---

## Summary

| Step | What happened |
|---|---|
| 1 | DuckLake table created and visible as a PostgreSQL foreign table |
| 2 | pg_trickle automatically detected the DuckLake FDW and chose `DUCKLAKE_CHANGE_FEED` |
| 3 | Differential refresh processed only the 100 changed rows, not all 10 million |
| 4 | Compaction policy configured to handle snapshot expiry gracefully |
| 5 | Frontier snapshot_id is visible and advances monotonically |

See [Tutorial 6](tutorial-sub-millisecond-inlined-cdc.md) for the inlined-data fast path.
