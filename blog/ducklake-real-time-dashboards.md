[← Back to Blog Index](https://trickle-labs.github.io/pg-trickle/blog/) | [Documentation](https://trickle-labs.github.io/pg-trickle/)

# Real-Time Dashboards on Your Data Lake

## DuckLake stores your lake in PostgreSQL. pg_trickle keeps your aggregations fresh inside that same database — no Kafka, no Flink.

---

DuckLake v1.0 (released April 2026) made a bold architectural choice: the
entire catalog of a data lake — which files exist, which snapshots are current,
what the schemas look like — lives in a standard SQL database. Most commonly,
that database is PostgreSQL.

pg_trickle is a PostgreSQL extension that keeps SQL query results fresh
incrementally. It runs inside the same PostgreSQL instance. When DuckLake stores
its catalog in PostgreSQL, pg_trickle can watch DuckLake-backed tables and
maintain aggregations in sub-second latency — from inside the catalog database
itself.

This tutorial builds a live Grafana dashboard powered entirely by a pg_trickle
stream table sitting on top of a DuckLake event table.

---

## What You'll Build

A synthetic e-commerce event stream writes purchase events into a DuckLake
table at high throughput. A pg_trickle stream table computes per-minute revenue
and per-product purchase counts incrementally. Grafana visualises the results
live with a five-second auto-refresh.

The architecture looks like this:

```
DuckDB load generator
        │  (writes events to DuckLake via PostgreSQL catalog)
        ▼
PostgreSQL (DuckLake catalog)
        │  (foreign-table polling CDC)
        ▼
pg_trickle stream table: revenue_by_minute
        │  (standard SQL table, always fresh)
        ▼
Grafana dashboard
```

No Kafka. No Flink. No separate streaming cluster. One PostgreSQL instance, one
S3 bucket (or MinIO for local dev), and a Grafana container.

---

## Prerequisites

- PostgreSQL 18 with pg_trickle installed
- DuckDB CLI with the `ducklake` extension
- MinIO (or AWS S3) for Parquet storage
- Grafana (any recent version)
- A DuckLake catalog database pointing at your PostgreSQL instance

This tutorial uses MinIO for local development. For production, substitute an
S3 bucket.

---

## Step 1: Set Up DuckLake with PostgreSQL Catalog

Start a MinIO container for object storage:

```bash
docker run -d --name minio \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  -p 9000:9000 -p 9001:9001 \
  minio/minio server /data --console-address ":9001"
```

Create the DuckLake schema in PostgreSQL and attach DuckDB:

```sql
-- In PostgreSQL: create a database for the DuckLake catalog
CREATE DATABASE lake_catalog;
\c lake_catalog
```

In DuckDB:

```sql
-- Attach PostgreSQL as the DuckLake catalog
ATTACH 'ducklake:postgres:host=localhost dbname=lake_catalog user=postgres password=secret'
    AS my_lake (TYPE DUCKLAKE, DATA_PATH 's3://my-events-lake/');

-- Configure S3/MinIO credentials
SET s3_endpoint = 'localhost:9000';
SET s3_access_key_id = 'minioadmin';
SET s3_secret_access_key = 'minioadmin';
SET s3_use_ssl = false;
SET s3_url_style = 'path';

-- Use the lake and create the events table
USE my_lake;
CREATE TABLE ecommerce_events (
    event_id    BIGINT,
    user_id     INT,
    product_id  INT,
    event_type  VARCHAR,   -- 'view', 'add_to_cart', 'purchase'
    revenue_usd DECIMAL(10,2),
    occurred_at TIMESTAMPTZ
);
```

---

## Step 2: Expose the DuckLake Table as a PostgreSQL Foreign Table

DuckLake manages its data through the DuckDB engine, but the catalog lives in
PostgreSQL. For pg_trickle to watch the table, we expose it as a PostgreSQL
foreign table using `duckdb_fdw` or by using the DuckLake inlined-data path
(for small writes).

For the tutorial, we use the foreign-table polling path:

```sql
-- In lake_catalog database: install the duckdb FDW
CREATE EXTENSION IF NOT EXISTS postgres_fdw;  -- or duckdb_fdw when available

-- For simplicity, use a materialized bridge table
-- DuckDB writes events to ecommerce_events; a periodic job copies recent rows
-- into a local PostgreSQL table that pg_trickle can watch natively.
CREATE TABLE events_bridge (
    event_id    BIGINT PRIMARY KEY,
    user_id     INT,
    product_id  INT,
    event_type  TEXT,
    revenue_usd NUMERIC(10,2),
    occurred_at TIMESTAMPTZ
);
```

> **Note on DuckLake inlined data:** When DuckLake is configured for data
> inlining (the default for small writes), rows written by DuckDB land directly
> in a PostgreSQL table named `ducklake_inlined_data_table_<id>_<version>`.
> pg_trickle can attach triggers to those tables directly for sub-millisecond
> CDC. See [DuckLake's `table_changes()` Meets pg_trickle's DVM Engine](ducklake-table-changes-dvm.md)
> for the deep-dive on that path.

For this tutorial we use `events_bridge` as the pg_trickle source, populated
by a simple INSERT feed from the DuckDB load generator.

---

## Step 3: Create the Stream Table

```sql
-- Enable pg_trickle
CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- Create the incremental aggregation: revenue per minute and per product
SELECT pgtrickle.create_stream_table(
    name         => 'revenue_by_minute',
    query        => $$
        SELECT
            date_trunc('minute', occurred_at) AS minute,
            product_id,
            SUM(revenue_usd)   AS total_revenue,
            COUNT(*)           AS purchase_count
        FROM events_bridge
        WHERE event_type = 'purchase'
        GROUP BY date_trunc('minute', occurred_at), product_id
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

The `DIFFERENTIAL` mode means pg_trickle computes only the delta on each
refresh. When the load generator writes 100 new purchases, pg_trickle processes
those 100 rows — not the entire events table. The aggregation stays fresh with a
fraction of the CPU and I/O cost of a full recompute.

---

## Step 4: Run the Load Generator

The load generator inserts synthetic purchase events into `events_bridge` (which
mimics what DuckDB writes into the DuckLake table):

```sql
-- Insert a batch of events (run this in a loop or via pg_cron)
INSERT INTO events_bridge (event_id, user_id, product_id, event_type, revenue_usd, occurred_at)
SELECT
    gen_random_uuid()::bigint,
    (random() * 1000)::int,
    (random() * 50 + 1)::int,
    'purchase',
    (random() * 200 + 5)::numeric(10,2),
    now() - (random() * interval '5 minutes')
FROM generate_series(1, 50);
```

After five seconds, check the stream table:

```sql
SELECT * FROM revenue_by_minute
ORDER BY minute DESC, total_revenue DESC
LIMIT 10;
```

The results should show revenue aggregated by minute and product, updated
incrementally.

---

## Step 5: Connect Grafana

In Grafana:

1. Add a PostgreSQL data source pointing at `lake_catalog`.
2. Create a new panel with this query:

```sql
SELECT
    $__timeGroupAlias(minute, '1m'),
    SUM(total_revenue) AS "Total Revenue (USD)"
FROM revenue_by_minute
WHERE $__timeFilter(minute)
GROUP BY 1
ORDER BY 1;
```

3. Set the panel type to **Time series** and the refresh interval to **5s**.

You now have a live Grafana dashboard that updates every five seconds showing
cumulative revenue from your DuckLake event store — computed incrementally by
pg_trickle without ever scanning the full events table.

---

## Step 6: Add a Second Stream Table for Funnel Analysis

```sql
-- Funnel counts per product: views → add_to_cart → purchase
SELECT pgtrickle.create_stream_table(
    name         => 'funnel_by_product',
    query        => $$
        SELECT
            product_id,
            COUNT(*) FILTER (WHERE event_type = 'view')        AS views,
            COUNT(*) FILTER (WHERE event_type = 'add_to_cart') AS add_to_cart,
            COUNT(*) FILTER (WHERE event_type = 'purchase')    AS purchases,
            ROUND(
                100.0 * COUNT(*) FILTER (WHERE event_type = 'purchase')
                       / NULLIF(COUNT(*) FILTER (WHERE event_type = 'view'), 0),
                2
            ) AS conversion_pct
        FROM events_bridge
        GROUP BY product_id
    $$,
    schedule     => '10s',
    refresh_mode => 'DIFFERENTIAL'
);
```

Add a second Grafana panel:

```sql
SELECT
    product_id,
    views,
    add_to_cart,
    purchases,
    conversion_pct
FROM funnel_by_product
ORDER BY purchases DESC
LIMIT 20;
```

This gives you a live conversion funnel table that updates every 10 seconds —
incrementally maintained as new events arrive from DuckLake.

---

## Performance Notes

- **Incremental cost:** pg_trickle processes only new/changed rows per refresh
  cycle. For 1,000 new events in a 50,000-event history, the delta work is
  proportional to 1,000 rows, not 50,000.
- **Schedule tuning:** Set the schedule to `'5s'` for dashboards, `'30s'` for
  less time-sensitive aggregations, or `'1m'` for overnight batch-style jobs.
- **Foreign-table polling:** If you use a foreign table (not a bridge table),
  each refresh cycle scans the full foreign table. Prefer the bridge table
  pattern for large event volumes.
- **DuckLake inlined data:** For write workloads under the inlining threshold
  (~10 rows per write), DuckLake writes directly into PostgreSQL tables.
  pg_trickle can attach triggers to these tables for true sub-millisecond CDC —
  no polling needed.

---

## Summary

With pg_trickle and DuckLake sharing the same PostgreSQL instance:

- Your data lake's catalog lives in PostgreSQL (DuckLake).
- Your aggregations live in PostgreSQL (pg_trickle stream tables).
- Your dashboard reads from PostgreSQL (Grafana SQL data source).
- Everything updates incrementally — no full scans, no batch jobs, no
  external streaming infrastructure.

DuckLake's own roadmap lists "Materialized views and incremental maintenance"
as a future feature. pg_trickle already delivers it, today, from inside the
same database.

---

*See also:*
- [Tutorial: The Modern Data Stack in One Box](ducklake-modern-data-stack.md)
- [Tutorial: Monitoring Your DuckLake with pg_trickle](ducklake-monitoring.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](ducklake-ivm-missing-piece.md)
- [Blog: DuckLake's `table_changes()` Meets pg_trickle's DVM Engine](ducklake-table-changes-dvm.md)
- [Foreign Tables as Stream Table Sources](foreign-table-sources.md)
