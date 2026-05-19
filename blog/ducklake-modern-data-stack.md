[← Back to Blog Index](https://trickle-labs.github.io/pg-trickle/blog/) | [Documentation](https://trickle-labs.github.io/pg-trickle/)

# The Modern Data Stack in One Box

## Replace five infrastructure systems with one PostgreSQL instance and an S3 bucket

---

The modern data stack has a well-known problem: it requires too many systems.
A typical company running analytics in 2025 maintains:

1. **An OLTP database** (PostgreSQL) — the application database
2. **A CDC pipeline** (Debezium or AWS DMS) — captures changes from the OLTP DB
3. **A message broker** (Kafka) — buffers the change stream
4. **A stream processor** (Flink or Spark Streaming) — computes rolling aggregations
5. **A data lake** (Apache Iceberg on S3) — stores historical data cheaply
6. **A catalog service** (Apache Polaris or Nessie) — makes the lake queryable
7. **An analytics engine** (DuckDB, Trino, or Spark) — ad-hoc queries
8. **A workflow orchestrator** (Airflow or Dagster) — wires everything together

Each system has its own operations team, its own failure modes, and its own
cloud bill. A rough industry estimate puts the total cost of running this stack
for a mid-size company at $40,000–$200,000 per month.

This tutorial shows how to replace most of that with one PostgreSQL instance
and an S3 bucket — using pg_trickle for incremental aggregations and DuckLake
for lakehouse storage. No Kafka. No Flink. No JVM. No separate orchestration.

---

## The Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                          PostgreSQL 18                           │
│                                                                   │
│  ┌─────────────────┐    ┌───────────────────────────────────┐    │
│  │  OLTP Tables    │    │         pg_trickle                │    │
│  │  (orders,       │───▶│  Stream tables:                   │    │
│  │   products,     │    │  • revenue_by_day                 │    │
│  │   customers)    │    │  • inventory_forecast             │    │
│  └─────────────────┘    │  • customer_lifetime_value        │    │
│                         │  • product_performance            │    │
│  ┌─────────────────┐    └────────────────┬──────────────────┘    │
│  │  DuckLake       │                     │                       │
│  │  Catalog        │◀────────────────────┘  (results published)  │
│  │  (~28 tables)   │                                             │
│  └─────────────────┘                                             │
└──────────────────────────────┬──────────────────────────────────┘
                                │
                         S3 / MinIO
                      (Parquet data files)
                                │
             ┌──────────────────┼──────────────────┐
             │                  │                  │
          DuckDB             Apache Spark        Trino
        (ad-hoc queries)   (batch processing)  (SQL queries)
```

The one architectural insight that makes this possible: **DuckLake stores its
entire catalog in PostgreSQL.** The metadata tables for your data lake are just
regular PostgreSQL tables. pg_trickle lives in that same database. There are no
network hops, no serialisation costs, no authentication boundaries between the
incremental compute engine and the lake catalog.

---

## What You'll Build

This tutorial builds the OLTP-to-lake pipeline for a small SaaS company:

- **OLTP tables:** `orders`, `products`, `customers`
- **pg_trickle stream tables:** real-time revenue, inventory, and CLV aggregations
- **DuckLake tables:** historical archive of the same data in Parquet on S3
- **DuckDB queries:** ad-hoc analytics over the full history plus the live aggregations

---

## Prerequisites

- PostgreSQL 18 with pg_trickle installed
- DuckDB CLI with the `ducklake` extension
- MinIO or AWS S3
- Python 3.11+ (for the data generator script)

---

## Step 1: Create the OLTP Tables

```sql
-- Standard OLTP schema
CREATE TABLE customers (
    id         SERIAL PRIMARY KEY,
    email      TEXT NOT NULL UNIQUE,
    name       TEXT NOT NULL,
    plan       TEXT NOT NULL DEFAULT 'free',   -- 'free', 'pro', 'enterprise'
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE products (
    id          SERIAL PRIMARY KEY,
    sku         TEXT NOT NULL UNIQUE,
    name        TEXT NOT NULL,
    category    TEXT NOT NULL,
    price_usd   NUMERIC(10,2) NOT NULL,
    stock_count INT NOT NULL DEFAULT 0
);

CREATE TABLE orders (
    id          BIGSERIAL PRIMARY KEY,
    customer_id INT NOT NULL REFERENCES customers(id),
    product_id  INT NOT NULL REFERENCES products(id),
    quantity    INT NOT NULL DEFAULT 1,
    total_usd   NUMERIC(10,2) NOT NULL,
    status      TEXT NOT NULL DEFAULT 'pending',   -- 'pending', 'shipped', 'cancelled'
    ordered_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
```

---

## Step 2: Create pg_trickle Stream Tables

```sql
CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- Revenue by day and product category
SELECT pgtrickle.create_stream_table(
    name         => 'revenue_by_day',
    query        => $$
        SELECT
            date_trunc('day', o.ordered_at)  AS day,
            p.category,
            SUM(o.total_usd)                 AS revenue_usd,
            COUNT(*)                         AS order_count,
            COUNT(DISTINCT o.customer_id)    AS unique_customers
        FROM orders o
        JOIN products p ON p.id = o.product_id
        WHERE o.status = 'shipped'
        GROUP BY date_trunc('day', o.ordered_at), p.category
    $$,
    schedule     => '10s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Customer lifetime value
SELECT pgtrickle.create_stream_table(
    name         => 'customer_lifetime_value',
    query        => $$
        SELECT
            c.id,
            c.email,
            c.plan,
            SUM(o.total_usd)             AS lifetime_value_usd,
            COUNT(*)                     AS total_orders,
            MAX(o.ordered_at)            AS last_order_at,
            MIN(o.ordered_at)            AS first_order_at
        FROM customers c
        LEFT JOIN orders o ON o.customer_id = c.id AND o.status = 'shipped'
        GROUP BY c.id, c.email, c.plan
    $$,
    schedule     => '30s',
    refresh_mode => 'DIFFERENTIAL'
);

-- Inventory alert: products with low stock relative to recent demand
SELECT pgtrickle.create_stream_table(
    name         => 'inventory_health',
    query        => $$
        SELECT
            p.id,
            p.sku,
            p.name,
            p.stock_count,
            COALESCE(recent.units_sold_7d, 0)  AS units_sold_7d,
            CASE
                WHEN p.stock_count = 0 THEN 'OUT_OF_STOCK'
                WHEN p.stock_count < COALESCE(recent.units_sold_7d, 0) THEN 'LOW_STOCK'
                ELSE 'OK'
            END AS stock_status
        FROM products p
        LEFT JOIN (
            SELECT product_id, SUM(quantity) AS units_sold_7d
            FROM orders
            WHERE status = 'shipped'
              AND ordered_at >= now() - interval '7 days'
            GROUP BY product_id
        ) recent ON recent.product_id = p.id
    $$,
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Step 3: Set Up DuckLake for Historical Storage

In DuckDB, set up the DuckLake catalog pointing at your PostgreSQL instance:

```sql
-- DuckDB shell
INSTALL ducklake;
LOAD ducklake;

-- Attach PostgreSQL as the catalog database
ATTACH 'ducklake:postgres:host=localhost dbname=your_db user=postgres password=secret'
    AS company_lake (TYPE DUCKLAKE, DATA_PATH 's3://company-data-lake/');

USE company_lake;

-- Create historical mirror tables in DuckLake
CREATE TABLE orders_archive AS SELECT * FROM postgres_query(
    'host=localhost dbname=your_db user=postgres password=secret',
    'SELECT * FROM orders'
);

CREATE TABLE revenue_archive AS SELECT * FROM postgres_query(
    'host=localhost dbname=your_db user=postgres password=secret',
    'SELECT * FROM revenue_by_day'
);
```

The `revenue_archive` table in DuckLake is a snapshot of the pg_trickle stream
table results. You can query it from any DuckDB client, Spark, or Trino — or
join it against the live `revenue_by_day` stream table via `postgres_scanner`.

---

## Step 4: Query Across Live and Historical Data

With DuckDB's `postgres_scanner`, you can join DuckLake's historical Parquet
data with pg_trickle's live stream tables in a single query:

```sql
-- DuckDB shell: join historical archive with live stream table results
WITH historical AS (
    SELECT * FROM company_lake.revenue_archive
    WHERE day < current_date - 30
),
live AS (
    SELECT * FROM postgres_scan(
        'host=localhost dbname=your_db',
        'public', 'revenue_by_day'
    )
    WHERE day >= current_date - 30
)
SELECT
    category,
    SUM(revenue_usd) AS total_revenue_90d,
    COUNT(DISTINCT day) AS active_days
FROM (SELECT * FROM historical UNION ALL SELECT * FROM live)
GROUP BY category
ORDER BY total_revenue_90d DESC;
```

This query seamlessly combines:
- **Historical data**: old Parquet files in DuckLake (cheap, columnar, S3-stored)
- **Live data**: pg_trickle stream tables in PostgreSQL (sub-second fresh)

No ETL pipeline moves data between systems. No sync job runs at midnight. The
two datasets live in different physical locations but are queryable as one via
the shared PostgreSQL metadata layer that DuckLake provides.

---

## Step 5: Dashboard in Grafana

Connect Grafana to your PostgreSQL instance and query the stream tables directly:

```sql
-- Panel: Daily revenue trend (last 30 days)
SELECT
    $__timeGroupAlias(day, '1d'),
    category,
    SUM(revenue_usd) AS "Revenue (USD)"
FROM revenue_by_day
WHERE $__timeFilter(day)
GROUP BY 1, category
ORDER BY 1;
```

```sql
-- Panel: Customer value distribution by plan
SELECT
    plan,
    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY lifetime_value_usd) AS p50_ltv,
    PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY lifetime_value_usd) AS p90_ltv,
    AVG(lifetime_value_usd) AS avg_ltv
FROM customer_lifetime_value
GROUP BY plan;
```

```sql
-- Panel: Inventory alerts
SELECT sku, name, stock_count, units_sold_7d, stock_status
FROM inventory_health
WHERE stock_status != 'OK'
ORDER BY stock_status DESC, units_sold_7d DESC;
```

---

## System Comparison

| Task | Classic Stack | This Stack |
|------|--------------|------------|
| OLTP storage | PostgreSQL | PostgreSQL |
| CDC pipeline | Debezium + Kafka | pg_trickle (built in) |
| Stream processing | Flink / Spark Streaming | pg_trickle |
| Historical storage | Iceberg + catalog service | DuckLake on S3 |
| Ad-hoc analytics | Trino / Spark | DuckDB |
| Dashboards | Grafana → Kafka/Flink | Grafana → PostgreSQL |
| Orchestration | Airflow / Dagster | Not needed |
| Total systems | 7–9 | 2 |
| Estimated monthly cost | $40k–$200k | $500–$5k |

The cost difference is real. The complexity difference is visible in the
architecture diagram above: the classic stack has seven nodes with seven sets
of configuration, deployment pipelines, monitoring, and on-call runbooks. This
stack has one node for compute (PostgreSQL) and one for storage (S3).

---

## What You Lose

This simplified stack has genuine limitations:

- **Sub-100ms latency at extreme scale:** If you need single-digit millisecond
  refresh latency for millions of rows per second, the dedicated stream processors
  (Flink, Materialize) will outperform pg_trickle's refresh cycle approach.
- **Multi-database DAGs:** All stream tables must live in one PostgreSQL instance.
  Cross-database dependencies require federation via foreign tables.
- **Non-SQL sources:** Kafka topics, IoT device streams, and webhook feeds require
  a bridge layer to land data in PostgreSQL before pg_trickle can watch them.
- **Petabyte-scale DuckLake tables:** For enormous tables, the full-scan polling
  CDC path is too expensive. Use the DuckLake inlined-data trigger path or wait
  for the Phase 2 DuckLake change-feed adapter (planned for v0.65.0).

For most analytics workloads at a company processing under 10M events/day, none
of these limitations are binding constraints.

---

## Summary

The "modern data stack" complexity problem is real, but it is also largely
self-inflicted. DuckLake and pg_trickle combine two radical simplifications —
"the lake catalog is just SQL" and "incremental aggregations are just SQL" —
into a stack that fits in your head and on a single server.

One PostgreSQL instance. One S3 bucket. Sub-second fresh aggregations.
Historical queries over years of Parquet data. Live dashboards without a
streaming cluster. The full story is now just boring infrastructure.

---

*See also:*
- [Tutorial: Real-Time Dashboards on Your Data Lake](ducklake-real-time-dashboards.md)
- [Tutorial: Monitoring Your DuckLake with pg_trickle](ducklake-monitoring.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](ducklake-ivm-missing-piece.md)
- [Foreign Tables as Stream Table Sources](foreign-table-sources.md)
