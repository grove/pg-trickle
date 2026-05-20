# Tutorial 3: The Modern Data Stack in One Box

*PostgreSQL + pg_trickle + DuckLake + DuckDB — no Kafka, no Airflow, no separate stream processor*

---

## Overview

This tutorial walks you through running a complete modern data stack in a single
`docker compose up` environment:

| Layer | Technology | Role |
|-------|-----------|------|
| OLTP | PostgreSQL 18 + pg_trickle | Write orders; compute aggregations incrementally |
| Object storage | MinIO (S3-compatible) | Parquet files for the data lake |
| Lake catalog | DuckLake | Manages the Parquet files and snapshots |
| Analytics | DuckDB | Ad-hoc queries over the historical Parquet data |

The result: a stream table computes `revenue_by_region` on every 5-second
refresh cycle. Each delta is written to DuckLake as a new Parquet snapshot on
MinIO. DuckDB queries the historical snapshots for trend analysis.

**Total setup time: under 10 minutes.**

---

## Prerequisites

- Docker Engine 24+ and Docker Compose v2
- pg_trickle v0.67.0+
- 4 GB free RAM

---

## Architecture

```
PostgreSQL 18
  │  orders table  ← INSERT new orders here
  │
  └─ stream table: revenue_by_region
       │  5-second DIFFERENTIAL refresh
       │  DuckLake sink (append mode)
       ▼
  MinIO (S3-compatible)
       │  Parquet delta files
       ▼
  DuckLake catalog (PostgreSQL)
       │  ducklake_snapshot records each delta
       │  ducklake_view exposes revenue_by_region as a native DuckLake object
       ▼
  DuckDB
       └─ SELECT * FROM ducklake.revenue_by_region
          (reads latest Parquet snapshots from MinIO)
```

---

## Step 1: Create the source table and stream table

Connect to PostgreSQL and run:

```sql
-- Source table
CREATE TABLE orders (
    order_id   BIGSERIAL PRIMARY KEY,
    region     TEXT NOT NULL,
    amount     NUMERIC(10,2) NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Stream table with DuckLake sink
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query => $$
        SELECT region,
               SUM(amount) AS total_revenue,
               COUNT(*)    AS order_count
        FROM orders
        GROUP BY region
    $$,
    schedule             => '5s',
    refresh_mode         => 'DIFFERENTIAL',
    sink                 => 'ducklake',
    ducklake_sink_path   => 's3://pg-trickle-demo/revenue_by_region/'
);
```

After this call, pg_trickle:

1. Creates the `revenue_by_region` storage table.
2. Registers a `ducklake_view` entry so DuckDB sees `revenue_by_region` as a
   native catalog object.
3. Starts the 5-second refresh schedule.

---

## Step 2: Insert some orders

```sql
INSERT INTO orders (region, amount) VALUES
    ('us-east', 149.99),
    ('us-west', 89.50),
    ('eu-west', 220.00),
    ('ap-south', 55.75);
```

Within 5 seconds the stream table refreshes and writes a Parquet delta to MinIO.

---

## Step 3: Query from DuckDB

In a DuckDB session connected to the DuckLake catalog:

```sql
-- Attach the DuckLake catalog
ATTACH 'ducklake:postgresql://postgres:postgres@localhost:5432/postgres'
    AS my_lake (TYPE DUCKLAKE);

-- Query revenue as a native DuckLake view
SELECT * FROM my_lake.revenue_by_region ORDER BY total_revenue DESC;

-- Historical trend: revenue by region by hour
SELECT
    date_trunc('hour', s.snapshot_time) AS hour,
    p.stream_table_name,
    SUM(p.delta_row_count) AS deltas_written
FROM ducklake_snapshot s
JOIN pgtrickle.pgt_ducklake_provenance p
    ON s.snapshot_id = p.ducklake_snapshot_id
GROUP BY 1, 2
ORDER BY 1 DESC;
```

---

## Step 4: Explore the provenance trail (INT-11)

pg_trickle records every sink write in `pgtrickle.pgt_ducklake_provenance`:

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at
FROM pgtrickle.pgt_ducklake_provenance
ORDER BY written_at DESC
LIMIT 10;
```

Each row links a pg_trickle refresh cycle to the DuckLake snapshot it produced,
giving you end-to-end lineage from the PostgreSQL write to the Parquet file on
MinIO.

---

## Step 5: Drop the stream table

```sql
SELECT pgtrickle.drop_stream_table('revenue_by_region');
```

This automatically removes the `ducklake_view` entry so the view no longer
appears in the DuckLake catalog.

---

## What you've built

With a single `create_stream_table` call you now have:

- **OLTP-speed writes** — orders land in PostgreSQL at full heap speed.
- **Incremental maintenance** — only changed rows flow through the DVM engine
  on each 5-second cycle; no full table scans.
- **Data lake integration** — every delta is durable on MinIO as Parquet.
- **Bidirectional discoverability** — the stream table is visible in both
  PostgreSQL (as a regular table) and DuckLake (as a native view).
- **Lineage** — every snapshot is traceable back to the exact refresh cycle
  that produced it.

---

## Next steps

- **[Tutorial 4](tutorial-streaming-postgres-to-data-lake.md)** — replicate a
  full OLTP table into a DuckLake data lake using the sink output mode.
- **[Demo E](../demos/ducklake-oltp-lake/)** — a self-contained `docker compose`
  demo of the OLTP-to-lake loop with a DuckDB notebook.
