[← Back to Blog Index](https://trickle-labs.github.io/pg-trickle/blog/) | [Documentation](https://trickle-labs.github.io/pg-trickle/)

# Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM

## DuckLake's own roadmap lists "materialized views and incremental maintenance" as a future feature. pg_trickle already delivers it — from inside the same database.

---

The data lakehouse movement promised to solve a real problem: the cost and
complexity of maintaining separate OLTP and OLAP systems. Formats like Apache
Iceberg and Delta Lake got the storage and cataloging right. But they left one
major gap: **incremental view maintenance** (IVM). The ability to keep a
pre-computed aggregation fresh as the underlying data changes — without
recomputing from scratch every time.

DuckLake v1.0, released in April 2026, makes a bold architectural bet: the
catalog lives in SQL. Not in JSON manifests on S3. Not in a proprietary REST
service. In a standard SQL database — most often PostgreSQL. That single
architectural choice changes what is possible for IVM in a way that no previous
lakehouse format has.

This post explains why, and why pg_trickle is the natural answer.

---

## The IVM Gap in Lakehouse Formats

Every modern analytics workload needs aggregated results:

- Monthly revenue by product category
- Customer lifetime value, segmented by cohort
- Rolling 7-day click-through rates
- Per-tenant storage consumption

The traditional answer is "run a batch job once a day and cache the results."
The modern answer is "use a materialized view and refresh it on a schedule."
Both answers share the same flaw: **they recompute from the full dataset on
every refresh cycle.**

A table with 100 million rows that receives 10,000 new rows per minute does not
need its aggregates recomputed from all 100 million rows every minute. It needs
the aggregates updated to reflect the 10,000 new rows — the delta. IVM is the
technique that does this correctly and efficiently.

Doing IVM correctly is hard. You need to:
1. Detect exactly which rows changed (CDC).
2. Determine which downstream aggregations are affected by those changes.
3. Apply the algebraic inverse of the aggregate function to remove old
   contributions and add new ones — without reading the rows that didn't change.
4. Handle JOINs (which can produce phantom rows when joined tables change).
5. Handle HAVING clauses, subqueries, and complex SQL correctly.

Every major streaming SQL engine — Flink, Materialize, RisingWave, Ksql — has
spent years getting these rules right. They are all correct. They are all also
**separate, external systems** that you must deploy, operate, and connect to
your lake via a CDC pipeline.

---

## Why DuckLake Changes the Equation

Apache Iceberg and Delta Lake store their catalog in files on S3 — JSON
manifests, Avro metadata, snapshot pointers scattered across thousands of small
files. The catalog is not SQL-accessible. To build IVM on top of Iceberg, you
need to:

1. Poll the S3 catalog for new snapshot pointers.
2. Read the new metadata files from S3.
3. Decode which data files are new.
4. Read those Parquet files from S3.
5. Apply the delta.

Steps 1–4 happen before you even see the changed rows. This is why every IVM
system built on Iceberg uses Kafka as a mediator — you emit change events from
step 3 into a Kafka topic and let the stream processor handle step 5.

DuckLake eliminates steps 1–4. When DuckLake commits a snapshot, the catalog
update is a few SQL `INSERT` statements into regular PostgreSQL tables. The
changed rows are already visible in the same PostgreSQL database. The snapshot
pointer is a PostgreSQL row. The data file list is a PostgreSQL table.

If the IVM engine lives in the same PostgreSQL instance — and pg_trickle does —
there is no poll cycle, no S3 read, no Kafka topic. The IVM engine can watch
the DuckLake catalog tables directly, using PostgreSQL's own trigger mechanism
or WAL-based CDC, and maintain aggregations in the same transaction.

---

## How pg_trickle Fills the Gap

pg_trickle is a PostgreSQL 18 extension that maintains SQL query results
incrementally. You define a query once:

```sql
SELECT pgtrickle.create_stream_table(
    name         => 'revenue_by_product',
    query        => $$
        SELECT product_id, SUM(amount) AS total_revenue
        FROM events_bridge
        GROUP BY product_id
    $$,
    schedule     => '5s',
    refresh_mode => 'DIFFERENTIAL'
);
```

From that point on, `revenue_by_product` is a regular PostgreSQL table that
stays fresh. When new rows arrive in `events_bridge`, pg_trickle processes only
those rows — not the entire table. The differential dataflow engine applies the
delta rules for each SQL operator (SUM, COUNT, JOIN, GROUP BY) and updates the
result table in place.

The DuckLake integration requires no new pg_trickle features. DuckLake writes
rows into PostgreSQL tables (either as inlined data, or via its metadata
catalog). pg_trickle watches those tables and maintains aggregations. The entire
IVM pipeline runs inside PostgreSQL, with zero external infrastructure.

---

## What DuckLake's Own Roadmap Says

From the DuckLake public roadmap, under "Future Features":

> **Materialized views and incremental maintenance**
> Pre-computed views that are automatically kept in sync with the underlying
> data. *Currently looking for funding.*

DuckLake's team knows this feature is needed. They have not yet built it because
doing IVM correctly is genuinely hard and expensive to maintain. pg_trickle has
been working on this problem for over two years. The implementation is
production-ready.

The natural conclusion: for PostgreSQL-backed DuckLake deployments, pg_trickle
is the IVM engine DuckLake's roadmap is pointing at.

---

## Comparison with Alternative Approaches

| Approach | Works with DuckLake? | External infrastructure? | IVM quality |
|----------|---------------------|--------------------------|-------------|
| Batch recompute (pg_cron) | Yes | No | None (full scan) |
| Materialize / RisingWave | Yes, with Kafka CDC | Yes (separate cluster) | Excellent |
| Apache Flink | Yes, with Kafka CDC | Yes (JVM cluster) | Excellent |
| Snowflake Dynamic Tables | No (different system) | Snowflake account | Good |
| **pg_trickle** | **Yes, natively** | **No** | **Excellent** |

The key differentiator: pg_trickle is the only IVM engine that runs inside the
same database that hosts the DuckLake catalog. No CDC pipeline, no message
broker, no separate cluster, no cross-network latency.

---

## The Scalability Argument

DuckLake's design principle is "the catalog is small, the data is large." A
catalog with 1 million snapshots and 10 million data files still fits in a few
gigabytes of PostgreSQL storage. The Parquet data files on S3 might be
petabytes.

pg_trickle's IVM runs on the catalog side. The delta processing touches only
the PostgreSQL metadata — specifically, only the rows that changed in the last
refresh cycle. The physical Parquet files on S3 are never read by the IVM
engine.

For a DuckLake table that receives 1,000 row insertions per minute, the IVM
cost is proportional to 1,000 rows — not the total table size. For a 100-billion-
row DuckLake table, the IVM cost is still proportional to the delta, not the
history. This is the fundamental O(Δ) scaling property that makes pg_trickle
viable at data lake scale.

---

## What Comes Next

This release (v0.64.0) establishes the narrative and demonstrates the
integration with tutorials, demos, and documentation. The current integration
uses the foreign-table polling path — pg_trickle watches a bridge table that
mirrors DuckLake writes.

The next phase (v0.65.0) will deliver a native DuckLake change-feed adapter
that consumes DuckLake's `table_changes()` function directly, eliminating the
polling overhead and enabling sub-second latency even for large DuckLake tables.
The Phase 2 adapter will turn pg_trickle into a true production-grade IVM
engine for DuckLake — no bridge table, no polling, no full scans.

---

## Summary

DuckLake made the lakehouse catalog SQL-accessible. That one architectural
decision creates the conditions for genuine incremental view maintenance, because
the catalog, the delta, and the IVM engine can all run in the same database
process. pg_trickle is that IVM engine.

DuckLake's roadmap says "looking for funding" for IVM. pg_trickle has already
built it. The two projects fit together as naturally as they do because they made
the same fundamental bet: SQL-first, PostgreSQL-inside, no external infrastructure
required.

If you run DuckLake on PostgreSQL, you already have access to the best IVM
engine available for your data lake. It is called pg_trickle, and it is free.

---

*See also:*
- [Tutorial: Real-Time Dashboards on Your Data Lake](ducklake-real-time-dashboards.md)
- [Tutorial: The Modern Data Stack in One Box](ducklake-modern-data-stack.md)
- [Blog: DuckLake's `table_changes()` Meets pg_trickle's DVM Engine](ducklake-table-changes-dvm.md)
- [Foreign Tables as Stream Table Sources](foreign-table-sources.md)
