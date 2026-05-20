# Demo B — DuckLake Time-Travel Debugging

> **Roll back a stream table to any past snapshot and watch it rewind deterministically.**

---

## What This Demo Shows

This demo starts a pg_trickle stream table over a DuckLake change-feed source. A synthetic
event generator continuously inserts data into DuckLake. The stream table refreshes every few
seconds via the O(Δ) change-feed adapter.

At any point you can:

1. **Pause** the scheduler.
2. **Rewind** `last_consumed_snapshot_id` to a past snapshot.
3. **Replay** from that snapshot — the stream table rewinds deterministically to its past state.

This is a powerful debugging tool: if you notice a data quality issue in a stream table,
you can rewind to the snapshot just before the problem appeared and inspect the data.

---

## Quick Start

```bash
docker compose up -d
```

Wait for the stream table to be populated (≈ 10 seconds), then open a `psql` session:

```bash
docker compose exec postgres psql -U postgres -d timetravel_demo
```

---

## Architecture

```
┌──────────────────────────────────────────────────────┐
│ PostgreSQL (pg_trickle v0.65.0+)                    │
│                                                       │
│  DuckLake FDW foreign table: lake.events              │
│      ↓ table_changes(from_snap, to_snap)              │
│  Change buffer: pgtrickle_changes.changes_<oid>       │
│      ↓ O(Δ) differential refresh                     │
│  Stream table: public.event_summary                   │
│      ↓ snapshot frontier: { snapshot_id: N }          │
│  Queryable at any past snapshot                       │
└──────────────────────────────────────────────────────┘
         ↑
         │ INSERT batches every 2 s
┌────────────────────┐
│ Event generator    │
└────────────────────┘
```

---

## Time-Travel Walkthrough

### 1. Check the Current Snapshot

```sql
SELECT
    frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id' AS current_snapshot,
    (SELECT COUNT(*) FROM event_summary) AS current_row_count
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'event_summary';
```

### 2. Record a "Good" Snapshot ID

```sql
-- Save the current snapshot ID before introducing a "bug"
SELECT (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint AS good_snapshot
FROM pgtrickle.pgt_stream_tables
WHERE pgt_name = 'event_summary';
-- Example: good_snapshot = 42
```

### 3. Wait for More Data (Simulate Bug Window)

Wait 10–20 seconds for more data to arrive, then:

```sql
-- Note the new (bugged) state
SELECT COUNT(*) AS bugged_row_count FROM event_summary;
```

### 4. Pause the Scheduler and Rewind

```sql
-- Pause all refreshes
SELECT pgtrickle.pause_scheduler(ARRAY['event_summary']);

-- Rewind the frontier snapshot_id to the "good" snapshot
UPDATE pgtrickle.pgt_stream_tables
SET frontier = jsonb_set(
    frontier,
    '{sources,ducklake:lake.events,snapshot_id}',
    '42'::jsonb  -- replace 42 with your good_snapshot value
)
WHERE pgt_name = 'event_summary';

-- Trigger a FULL refresh to rebuild from the rewound frontier
SELECT pgtrickle.refresh('event_summary', 'FULL');
```

### 5. Verify the Rewind

```sql
-- The stream table is now back at the state it had at snapshot 42
SELECT COUNT(*) AS rewound_row_count FROM event_summary;
SELECT (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint AS snapshot
FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'event_summary';
```

### 6. Resume from the Rewound Point

```sql
SELECT pgtrickle.resume_scheduler(ARRAY['event_summary']);
-- The stream table will now replay forwards from snapshot 42.
```

---

## Configuration

| Variable | Default | Description |
|---|---|---|
| `BATCH_SIZE` | `50` | Rows inserted per generator tick |
| `INTERVAL_MS` | `2000` | Milliseconds between generator ticks |
| `POSTGRES_PORT` | `5432` | Host port for PostgreSQL |

---

## Stopping the Demo

```bash
docker compose down -v
```
