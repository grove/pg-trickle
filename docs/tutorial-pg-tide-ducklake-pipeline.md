# INT-10 Tutorial: pg-tide DuckLake Pipeline

*Use pg-tide's transactional relay to stream data from pg_trickle stream tables into DuckLake*

---

## Overview

[pg-tide](https://github.com/trickle-labs/pg-tide) is a standalone transactional
outbox, inbox, and relay extension for PostgreSQL. Combined with pg_trickle, it
provides a fully transactional pipeline from stream table deltas to DuckLake
objects on object storage — with at-least-once delivery guarantees.

```
orders table
  │  (INSERT)
  ▼
pg_trickle stream table: revenue_by_region
  │  (DIFFERENTIAL refresh, 5s)
  ▼
pg-tide outbox  ← attach_outbox() wires the stream table CDC to pg-tide
  │  (transactional queue, SKIP LOCKED delivery)
  ▼
pg-tide relay
  │  sink_type => 'ducklake'
  ▼
DuckLake catalog + MinIO
  └─ Parquet delta files, queryable from DuckDB
```

**When to prefer this pattern over the direct sink:**

| Pattern | Best for |
|---------|---------|
| Direct sink (`sink => 'ducklake'`) | Simple replication, best-effort delivery |
| pg-tide relay | Transactional guarantees, exactly-once fan-out to multiple sinks |

---

## Prerequisites

- PostgreSQL 18 with pg_trickle v0.67.0+
- pg-tide v0.22.0+ (`CREATE EXTENSION pg_tide`)
- DuckLake installed (`CREATE EXTENSION ducklake`)
- MinIO or S3 bucket

---

## Step 1: Install extensions

```sql
CREATE EXTENSION IF NOT EXISTS pg_tide;
CREATE EXTENSION IF NOT EXISTS pg_trickle;
```

---

## Step 2: Create source and stream tables

```sql
CREATE TABLE orders (
    order_id   BIGSERIAL PRIMARY KEY,
    region     TEXT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);

SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query => $$
        SELECT region, SUM(amount) AS total_revenue
        FROM orders
        GROUP BY region
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Step 3: Attach a pg-tide outbox

```sql
SELECT pgtrickle.attach_outbox('revenue_by_region');
```

This call:
1. Creates a pg-tide outbox table for `revenue_by_region` change events.
2. Wires the stream table CDC so every differential delta is enqueued
   transactionally.
3. The outbox guarantees that every change is delivered at least once, even if
   the destination (DuckLake) is temporarily unavailable.

---

## Step 4: Configure pg-tide relay to DuckLake

```sql
SELECT tide.relay_set_outbox(
    outbox_name  => 'revenue_by_region',
    sink_type    => 'ducklake',
    sink_options => jsonb_build_object(
        'catalog_db',   'postgres',
        'data_path',    's3://my-lake/revenue_by_region/',
        's3_region',    'us-east-1',
        's3_access_key', 'YOUR_KEY',
        's3_secret_key', 'YOUR_SECRET'
    )
);
```

The relay runs in a background worker that polls the outbox, serialises each
batch to Parquet, uploads to S3, and records the DuckLake snapshot — all in a
single PostgreSQL transaction. If the upload fails, the outbox message stays
unacknowledged and the relay retries on the next poll.

---

## Step 5: Verify end-to-end flow

Insert some data and watch it flow:

```sql
INSERT INTO orders (region, amount) VALUES
    ('us-east', 99.00), ('eu-west', 155.50);

-- Wait 5 seconds, then check the provenance trail
SELECT *
FROM pgtrickle.pgt_ducklake_provenance
WHERE stream_table_name = 'revenue_by_region'
ORDER BY written_at DESC LIMIT 5;
```

---

## Difference from direct sink

With `attach_outbox()` + pg-tide relay, the write to DuckLake happens in a
separate transaction from the pg_trickle refresh. This means:

- **Guaranteed delivery** — the outbox record persists even if the DuckLake
  write fails; the relay retries automatically.
- **Fan-out** — you can configure multiple relay destinations (DuckLake,
  Kafka, SQS) for the same outbox.
- **Decoupled latency** — the refresh cycle is not blocked by the sink write;
  the relay runs asynchronously.

---

## Cleanup

```sql
SELECT tide.relay_remove_outbox('revenue_by_region');
SELECT pgtrickle.drop_stream_table('revenue_by_region');
```

---

## Next steps

- [pg-tide documentation](https://github.com/trickle-labs/pg-tide)
- [Tutorial 3: Modern Data Stack in One Box](tutorial-modern-data-stack-one-box.md)
- [Tutorial 4: Streaming PostgreSQL to a Data Lake](tutorial-streaming-postgres-to-data-lake.md)
