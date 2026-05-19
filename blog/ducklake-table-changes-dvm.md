[← Back to Blog Index](https://trickle-labs.github.io/pg-trickle/blog/) | [Documentation](https://trickle-labs.github.io/pg-trickle/)

# DuckLake's `table_changes()` Meets pg_trickle's DVM Engine

## A wire-level tour of how DuckLake's change-feed format maps to pg_trickle's internal change-buffer model — and why the mapping is almost perfect.

---

DuckLake ships with a built-in change data capture API:

```sql
-- DuckDB: get all changes to 'events' between two snapshots
SELECT * FROM table_changes('my_lake.events', 100, 200);
```

The output is a delta stream: rows tagged as `insert`, `delete`,
`update_preimage`, or `update_postimage`. This is exactly the format that
incremental view maintenance engines want to consume. This post dissects the
format row by row and explains how it maps to pg_trickle's internal
change-buffer schema — and what the Phase 2 DuckLake adapter will do with it.

---

## DuckLake's Change Feed API

DuckLake provides three functions for reading deltas:

| Function | Returns | When to use |
|----------|---------|------------|
| `table_changes(table, from_snapshot, to_snapshot)` | INSERT + DELETE + UPDATE (preimage + postimage) | Full bidirectional delta — required for correct IVM |
| `table_insertions(table, from_snapshot, to_snapshot)` | INSERT rows only | Append-only aggregations (COUNT, SUM without DELETEs) |
| `table_deletions(table, from_snapshot, to_snapshot)` | DELETE rows only | Soft-delete detection, retention enforcement |

The output of `table_changes()` for a simple table looks like:

```
rowid    | change_type      | user_id | amount | occurred_at
---------+------------------+---------+--------+--------------------
row-0001 | insert           |    42   | 99.00  | 2026-05-01 10:00:00
row-0002 | insert           |    17   | 45.50  | 2026-05-01 10:01:00
row-0001 | update_preimage  |    42   | 99.00  | 2026-05-01 10:00:00
row-0001 | update_postimage |    42   | 110.00 | 2026-05-01 10:00:00
row-0002 | delete           |    17   | 45.50  | 2026-05-01 10:01:00
```

Key observations:
- Every row has a stable `rowid` — a unique identifier that survives updates,
  compactions, and file movements.
- Updates arrive as a `(preimage, postimage)` pair, not as a single UPDATE row.
- Deletes are explicit rows with `change_type = 'delete'`.

This is a **signed multiset** representation. Each change carries implicit
multiplicity: an `insert` carries weight +1, a `delete` carries weight -1, an
`update_preimage` carries weight -1, and an `update_postimage` carries weight +1.

---

## pg_trickle's Internal Change-Buffer Schema

Inside pg_trickle, every source table gets a corresponding change buffer:

```sql
-- Conceptual schema (actual schema is generated per table OID)
CREATE TABLE pgtrickle_changes.changes_<source_oid> (
    pgt_change_id  BIGSERIAL NOT NULL,
    pgt_op         CHAR(1)  NOT NULL,  -- 'I' (insert), 'D' (delete), 'U' (update)
    pgt_lsn        pg_lsn,            -- WAL position (for WAL-CDC sources)
    pgt_txid       XID8,              -- Transaction ID
    pgt_weight     SMALLINT NOT NULL DEFAULT 1,  -- +1 or -1 for differential
    -- ... source table columns follow ...
);
```

The `pgt_weight` column is the key. It mirrors the signed-multiset semantics:

| DuckLake `change_type` | pg_trickle `pgt_op` + `pgt_weight` |
|------------------------|-------------------------------------|
| `insert`               | `'I'`, weight = +1                  |
| `delete`               | `'D'`, weight = -1                  |
| `update_preimage`      | `'U'`, weight = -1 (retract old)    |
| `update_postimage`     | `'U'`, weight = +1 (assert new)     |

The mapping is almost 1-to-1. A DuckLake `table_changes()` result can be
translated into pg_trickle change-buffer rows with a trivial SQL transform.

---

## The Differential Dataflow Engine (DVM)

Once the changes are in the change buffer, pg_trickle's DVM engine applies
the differential rules for each SQL operator:

**SUM:** If a row with `amount = 99.00` is deleted (weight -1), the SUM decreases
by 99.00. If a row with `amount = 110.00` is inserted (weight +1), the SUM
increases by 110.00. No full scan needed.

**COUNT:** Each inserted row contributes +1; each deleted row contributes -1.
A `COUNT(*)` aggregation updates in O(Δ) time.

**GROUP BY:** Only the groups whose keys appear in the delta are recomputed.
A delta affecting 3 of 1,000 groups causes 3 partial-aggregate updates, not
1,000.

**JOIN:** The tricky case. When table A changes, the delta must be joined against
the current state of table B, not a snapshot of B. pg_trickle handles this
correctly using the two-phase frontier approach, which guarantees that phantom
rows (rows that appear in a JOIN delta but should not be in the result) are
correctly retracted.

**HAVING:** HAVING clauses are handled by applying the filter after the
GROUP BY delta, treating the HAVING predicate as a multiplier on the group's
output weight.

---

## The `rowid` Advantage

DuckLake's `rowid` column is gold for IVM. In a standard PostgreSQL table,
pg_trickle uses `ctid` (the physical row location) or a user-defined primary
key to identify rows. For tables without a primary key, pg_trickle computes
a content hash to determine row identity.

DuckLake's `rowid` is better than `ctid` for two reasons:
1. It is stable across updates and compaction. A compacted file gets new
   physical row positions, but the same logical `rowid` values.
2. It is accessible to any engine reading the DuckLake table — DuckDB, Spark,
   pg_trickle — via the `table_changes()` API.

The Phase 2 adapter will use `rowid` as the primary key for DuckLake-source
change buffers, eliminating content hashing entirely and reducing per-row
CDC overhead.

---

## Data Inlining: The Sub-Millisecond Path

DuckLake has a "data inlining" feature: when a write touches fewer than a
configurable threshold of rows (default: 10), DuckLake skips writing a Parquet
file and stores the rows directly in a regular PostgreSQL table named
`ducklake_inlined_data_table_<table_id>_<snapshot_id>`.

For small writes, this means the data never leaves PostgreSQL. pg_trickle's
standard trigger-based CDC can attach to these inlined tables directly:

```sql
-- The inlined table is a regular PostgreSQL table
-- pg_trickle can watch it with a standard AFTER ROW trigger
-- (this is what the Phase 2 adapter will do automatically)
SELECT pgtrickle.create_stream_table(
    name   => 'events_summary',
    query  => $$
        SELECT date_trunc('hour', occurred_at), COUNT(*)
        FROM ducklake_inlined_data_table_42_1     -- table_id=42, latest snapshot
        GROUP BY 1
    $$,
    schedule => '1s',
    refresh_mode => 'DIFFERENTIAL'
);
```

For workloads where the average DuckLake write is small (typical for
application event streams), inlined data provides sub-millisecond CDC latency —
better than any polling-based approach.

---

## Snapshot Boundaries as Frontiers

pg_trickle uses a concept called the "frontier" to track which changes have
been processed. For PostgreSQL WAL sources, the frontier is an LSN (log
sequence number). For DuckLake sources, the natural frontier is the snapshot ID.

```
Last processed snapshot: 150
Current latest snapshot: 200
Delta to process: table_changes('events', 150, 200)
New frontier after processing: 200
```

Snapshot IDs are monotonically increasing integers — perfect frontier values.
Because DuckLake keeps millions of snapshots cheaply, the frontier can always
be resumed from exactly where it left off. There is no "missed update" risk.

---

## Why This Mapping Is Nearly Perfect

The combination of DuckLake's change feed and pg_trickle's DVM engine works
because both systems made the same foundational choice: **changes are signed
multisets, not state diffs**.

Iceberg and Delta Lake represent their changes as "file-level diffs" — "add
this Parquet file, remove that one." To get row-level changes, you must read
and decode the files. DuckLake represents changes at the row level, with
explicit weights (via the `change_type` column). pg_trickle's engine processes
row-level signed multisets. The shape matches.

The remaining gap is engineering: the Phase 2 adapter needs to:
1. Query `table_changes()` on a schedule using DuckDB's `postgres_scanner`.
2. Write the results into pg_trickle's change buffer format.
3. Update the frontier (snapshot ID) after each successful batch.

That is three SQL statements and a frontier management loop. The hard part —
the IVM rules for every SQL operator — is already implemented.

---

## Summary

DuckLake's `table_changes()` function produces a signed-multiset delta stream.
pg_trickle's DVM engine consumes signed-multiset delta streams. The mapping
between the two formats is one-to-one. DuckLake's `rowid` eliminates the
content-hashing overhead that is the bane of keyless CDC. DuckLake's snapshot
IDs are perfect frontier values.

The architecture was designed independently by two teams working on different
problems — but the design decisions converged to the same foundations. When two
systems independently choose signed multisets as their change representation,
connecting them is not integration engineering; it is just plumbing.

The Phase 2 adapter (planned for v0.65.0) will make the plumbing automatic.
Until then, the bridge-table pattern described in the other tutorials is a
fully functional fallback.

---

*See also:*
- [Tutorial: Real-Time Dashboards on Your Data Lake](ducklake-real-time-dashboards.md)
- [Tutorial: Monitoring Your DuckLake with pg_trickle](ducklake-monitoring.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](ducklake-ivm-missing-piece.md)
- [Foreign Tables as Stream Table Sources](foreign-table-sources.md)
