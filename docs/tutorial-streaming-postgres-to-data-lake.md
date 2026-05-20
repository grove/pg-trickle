# Tutorial 4: Streaming PostgreSQL to a Data Lake without Kafka

*Use pg_trickle's DuckLake sink to replicate OLTP data into Parquet on S3 — incrementally, with no external streaming infrastructure*

---

## Overview

Traditional approaches to replicating a PostgreSQL table into a data lake involve:

1. A Kafka cluster to capture changes.
2. A Kafka Connect connector (Debezium) to read WAL.
3. A stream processor (Flink, Spark Streaming) to transform and write Parquet.
4. A data lake catalog (Iceberg, Hudi, or DuckLake) to manage snapshots.

pg_trickle replaces steps 1–3 with a single SQL statement. The DuckLake sink
writes Parquet deltas directly from the PostgreSQL process, eliminating the
entire streaming infrastructure.

| Approach | Infra required | Latency | Setup time |
|----------|--------------|---------|------------|
| Kafka + Debezium + Flink | 3 separate systems | ~seconds | Days |
| pg_trickle DuckLake sink | PostgreSQL only | ~seconds | Minutes |

---

## Prerequisites

- PostgreSQL 18 with pg_trickle v0.67.0+
- DuckLake installed in PostgreSQL (`CREATE EXTENSION ducklake`)
- An S3-compatible object store (AWS S3, MinIO, etc.)
- DuckDB (optional, for querying results)

---

## Step 1: Choose what to replicate

For this tutorial we replicate a `customers` table. In practice this can be any
table — including one with millions of rows. pg_trickle uses differential
refresh so each sink cycle only writes the rows that changed, not the full table.

```sql
CREATE TABLE customers (
    customer_id BIGSERIAL PRIMARY KEY,
    email       TEXT NOT NULL UNIQUE,
    region      TEXT NOT NULL,
    tier        TEXT NOT NULL DEFAULT 'free',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Insert some test data
INSERT INTO customers (email, region, tier) VALUES
    ('alice@example.com', 'us-east', 'pro'),
    ('bob@example.com',   'eu-west', 'free'),
    ('carol@example.com', 'ap-south', 'enterprise');
```

---

## Step 2: Create a stream table with a DuckLake sink

```sql
-- Configure S3 credentials (or use IAM role / env var credential chain)
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_region = 'us-east-1';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_access_key = 'YOUR_ACCESS_KEY';
ALTER SYSTEM SET pg_trickle.ducklake_sink_s3_secret_key = 'YOUR_SECRET_KEY';
SELECT pg_reload_conf();

-- Create the replication stream table
SELECT pgtrickle.create_stream_table(
    'customers_lake',
    query => $$
        SELECT
            customer_id,
            email,
            region,
            tier,
            created_at,
            updated_at
        FROM customers
    $$,
    schedule           => '30s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://my-data-lake/customers/'
);
```

> **Sink mode:** The default mode is `'append'` — each 30-second cycle that
> has changes writes a new Parquet delta file to S3. DuckLake accumulates these
> deltas and presents a merged view via time-travel SQL.
>
> Use `'replace'` if you want each cycle to overwrite the previous full snapshot
> instead of accumulating deltas.

---

## Step 3: Verify the first write

After 30 seconds (or after the first refresh), check:

```sql
-- What did pg_trickle write?
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at
FROM pgtrickle.pgt_ducklake_provenance
WHERE stream_table_name = 'customers_lake'
ORDER BY written_at DESC;
```

---

## Step 4: Handle deletes (tombstone pattern)

DuckLake accumulates Parquet deltas; deleted rows are not automatically removed
from old files. To make deletes visible to DuckDB readers, add a `is_deleted`
flag column and filter it in your downstream queries:

```sql
-- Alter the stream table to include a deletion flag
SELECT pgtrickle.alter_stream_table(
    'customers_lake',
    query => $$
        SELECT
            customer_id,
            email,
            region,
            tier,
            created_at,
            updated_at,
            FALSE AS is_deleted   -- set manually for soft-delete patterns
        FROM customers
    $$
);
```

For hard deletes, use `ducklake_sink_mode = 'replace'` on a longer schedule
(e.g. `'1h'`) to produce periodic full-snapshot files that DuckLake clients
can time-travel past.

---

## Step 5: Query from DuckDB

```sql
-- Attach DuckLake
ATTACH 'ducklake:postgresql://localhost/postgres' AS lake (TYPE DUCKLAKE);

-- Latest customer state
SELECT * FROM lake.customers_lake WHERE is_deleted = FALSE;

-- Time-travel: customers as of 1 hour ago
SELECT * FROM lake.customers_lake
    AT (SNAPSHOT => (
        SELECT MAX(snapshot_id) FROM ducklake_snapshot
        WHERE snapshot_time <= now() - INTERVAL '1 hour'
    ));
```

---

## How pg_trickle handles the incremental writes

On each 30-second refresh cycle:

1. **CDC scan** — pg_trickle reads only the rows that changed since the last
   refresh (trigger-based or WAL-based, depending on configuration).
2. **DVM engine** — applies the SELECT query differentially over only the delta
   rows.
3. **Parquet serialisation** — writes the delta as a Parquet file with
   Snappy compression (configurable via `pg_trickle.ducklake_sink_compression`).
4. **S3 upload** — uploads the file to `s3://my-data-lake/customers/`.
5. **DuckLake catalog** — inserts `ducklake_data_file` + `ducklake_snapshot`
   rows atomically.
6. **View registration** — the `ducklake_view` entry for `customers_lake` is
   kept up to date with the latest query definition.
7. **Provenance** — a row is inserted into `pgtrickle.pgt_ducklake_provenance`
   linking the snapshot back to this refresh cycle.

If the S3 upload fails, no catalog entries are written and the next cycle
retries automatically.

---

## Cleanup

```sql
SELECT pgtrickle.drop_stream_table('customers_lake');
-- This automatically removes the ducklake_view entry.
```

---

## Next steps

- **[Tutorial 3](tutorial-modern-data-stack-one-box.md)** — the full modern
  data stack (OLTP + pg_trickle + DuckLake + DuckDB + Grafana).
- **[INT-10 tutorial](tutorial-pg-tide-ducklake-pipeline.md)** — use pg-tide's
  relay to stream data to DuckLake with transactional guarantees.
