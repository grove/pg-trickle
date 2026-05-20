# Demo E: OLTP-to-Lake Loop

A self-contained `docker compose up` demo of the full OLTP-to-data-lake loop:

1. Synthetic e-commerce orders land in a PostgreSQL `orders` table.
2. A pg_trickle stream table computes `revenue_by_region` incrementally.
3. The DuckLake sink publishes each delta to MinIO as a Parquet file.
4. A DuckDB notebook queries the historical Parquet files for trend analysis.

**End-to-end latency from order INSERT to DuckLake snapshot: typically under 2 seconds.**

---

## Quick Start

```bash
cd demos/ducklake-oltp-lake
docker compose up
```

- **JupyterLab**: http://localhost:8888 (token: `demo`) — open `oltp_lake_analysis.ipynb`
- **MinIO console**: http://localhost:9001 (minioadmin/minioadmin)

---

## Architecture

```
Order generator (Python)
  │  10 orders/second
  ▼
PostgreSQL 18 + pg_trickle
  │  orders table
  └─ stream table: revenue_by_region
       │  5-second DIFFERENTIAL refresh
       │  DuckLake sink (append mode)
       │  Provenance → pgtrickle.pgt_ducklake_provenance
       ▼
  MinIO (S3-compatible object store)
       │  Parquet delta files
       ▼
  DuckLake catalog
       │  ducklake_snapshot, ducklake_view
       ▼
  DuckDB (JupyterLab)
       └─ Trend analysis notebook
```

---

## Services

| Service | Port | Purpose |
|---------|------|---------|
| PostgreSQL 18 + pg_trickle | 5432 | OLTP source, stream table, DuckLake catalog |
| MinIO | 9000/9001 | S3-compatible object storage |
| JupyterLab | 8888 | DuckDB analytics notebook |
| Order generator | — | Python script producing 10 orders/s |

---

## The stream table

```sql
SELECT pgtrickle.create_stream_table(
    'revenue_by_region',
    query => $$
        SELECT
            region,
            date_trunc('minute', created_at) AS minute,
            SUM(amount)                       AS total_revenue,
            COUNT(*)                          AS order_count
        FROM orders
        GROUP BY region, date_trunc('minute', created_at)
    $$,
    schedule           => '5s',
    refresh_mode       => 'DIFFERENTIAL',
    sink               => 'ducklake',
    ducklake_sink_path => 's3://pg-trickle-demo/revenue_by_region/'
);
```

---

## Provenance query

```sql
SELECT
    stream_table_name,
    ducklake_snapshot_id,
    delta_row_count,
    written_at,
    written_at - LAG(written_at) OVER (ORDER BY written_at) AS cycle_duration
FROM pgtrickle.pgt_ducklake_provenance
WHERE stream_table_name = 'revenue_by_region'
ORDER BY written_at DESC
LIMIT 20;
```
