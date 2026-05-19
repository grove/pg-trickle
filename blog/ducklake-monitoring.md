[← Back to Blog Index](https://trickle-labs.github.io/pg-trickle/blog/) | [Documentation](https://trickle-labs.github.io/pg-trickle/)

# Monitoring Your DuckLake with pg_trickle

## DuckLake's 28 metadata tables are rich with operational signals. Build live alerts over them with stream tables.

---

Every DuckLake deployment comes with an operational intelligence layer built in:
the ~28 metadata tables that DuckLake maintains in PostgreSQL. These tables
record every snapshot, every data file, every schema change, every compaction
event, and every write operation. Together they are a complete operational audit
log of your data lake.

The problem is that these tables are not optimised for monitoring. Counting
"how many small files appeared in the last hour" requires a join across
`ducklake_data_file`, `ducklake_table_changes`, and `ducklake_snapshot` with
aggregate functions — a query you wouldn't want running on every Grafana
request.

pg_trickle solves this by computing those aggregations incrementally and
materialising the results. The monitoring queries become instant reads from a
pre-computed stream table instead of expensive multi-table scans.

---

## DuckLake's Key Metadata Tables

| Table | What it records |
|-------|----------------|
| `ducklake_snapshot` | Each committed transaction — the fundamental unit of DuckLake history |
| `ducklake_table_changes` | Which tables changed in each snapshot |
| `ducklake_data_file` | All Parquet data files: path, size, row count, min/max stats |
| `ducklake_deleted_file` | Files removed by compaction or deletion |
| `ducklake_schema` | Schema versions |
| `ducklake_table` | Table definitions |
| `ducklake_column_stats` | Per-column statistics for query planning |
| `ducklake_tag` | Named snapshot tags (time-travel anchors) |

The monitoring scenarios below use these tables as stream table sources.

---

## Scenario 1: Small-File Proliferation Alert

Parquet files under ~128 MB are typically a compaction problem: too many small
writes created too many small files, and file-open overhead will slow down
future scans. Track the small-file count per table and alert when it exceeds
a threshold.

```sql
-- Count Parquet files under 10 MB per table, updated every minute
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_small_file_counts',
    query        => $$
        SELECT
            dt.schema_name,
            dt.table_name,
            COUNT(*)                    AS small_file_count,
            SUM(df.file_size_bytes)     AS total_small_file_bytes,
            MAX(df.created_snapshot_id) AS latest_snapshot_id
        FROM ducklake_data_file df
        JOIN ducklake_table dt ON dt.table_id = df.table_id
        WHERE df.file_size_bytes < 10 * 1024 * 1024   -- < 10 MB
          AND df.deleted_snapshot_id IS NULL            -- still active
        GROUP BY dt.schema_name, dt.table_name
    $$,
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);
```

In Grafana, create an alert rule:

```sql
SELECT
    schema_name || '.' || table_name AS table_name,
    small_file_count
FROM ducklake_small_file_counts
WHERE small_file_count > 50
ORDER BY small_file_count DESC;
```

When any table accumulates more than 50 small files, Grafana fires an alert
telling you to run `CALL ducklake_compact('schema_name.table_name')` in DuckDB.

---

## Scenario 2: Snapshot Rate Monitoring

A sudden spike in snapshot creation rate usually indicates a runaway write loop,
a misconfigured ETL job, or a schema-migration script that created a new
snapshot per row. This stream table shows commits per minute per table.

```sql
-- Snapshot creation rate: commits per minute and per table
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_snapshot_rate',
    query        => $$
        SELECT
            date_trunc('minute', s.created_at)  AS minute,
            dt.schema_name,
            dt.table_name,
            COUNT(DISTINCT s.snapshot_id)        AS snapshots_in_minute,
            SUM(tc.rows_added)                   AS rows_added,
            SUM(tc.rows_deleted)                 AS rows_deleted
        FROM ducklake_snapshot s
        JOIN ducklake_table_changes tc ON tc.snapshot_id = s.snapshot_id
        JOIN ducklake_table dt ON dt.table_id = tc.table_id
        GROUP BY
            date_trunc('minute', s.created_at),
            dt.schema_name,
            dt.table_name
    $$,
    schedule     => '30s',
    refresh_mode => 'DIFFERENTIAL'
);
```

Grafana time-series panel:

```sql
SELECT
    $__timeGroupAlias(minute, '1m'),
    schema_name || '.' || table_name AS table_label,
    snapshots_in_minute AS "Snapshots / min"
FROM ducklake_snapshot_rate
WHERE $__timeFilter(minute)
ORDER BY 1, 2;
```

---

## Scenario 3: Storage Growth Curves

Track cumulative storage per table over time. Useful for capacity planning and
for identifying tables that are growing faster than expected.

```sql
-- Cumulative active storage per table, sampled every 5 minutes
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_storage_growth',
    query        => $$
        SELECT
            dt.schema_name,
            dt.table_name,
            SUM(df.file_size_bytes)  AS active_bytes,
            COUNT(*)                 AS active_file_count,
            SUM(df.row_count)        AS active_row_count,
            now()                    AS sampled_at
        FROM ducklake_data_file df
        JOIN ducklake_table dt ON dt.table_id = df.table_id
        WHERE df.deleted_snapshot_id IS NULL   -- active files only
        GROUP BY dt.schema_name, dt.table_name
    $$,
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Scenario 4: Per-Tenant Write Activity (Multi-Tenant Deployments)

For multi-tenant DuckLake deployments where each tenant writes to a separate
schema, track write volume per tenant to identify heavy writers, enforce
quotas, and bill accurately.

```sql
-- Tenant write activity: rows written per tenant per hour
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_tenant_write_activity',
    query        => $$
        SELECT
            dt.schema_name                          AS tenant_schema,
            date_trunc('hour', s.created_at)        AS hour,
            SUM(tc.rows_added)                      AS rows_added,
            SUM(tc.rows_deleted)                    AS rows_deleted,
            COUNT(DISTINCT s.snapshot_id)            AS commit_count,
            SUM(
                COALESCE(df.file_size_bytes, 0)
            ) AS bytes_written
        FROM ducklake_snapshot s
        JOIN ducklake_table_changes tc  ON tc.snapshot_id = s.snapshot_id
        JOIN ducklake_table dt          ON dt.table_id = tc.table_id
        LEFT JOIN ducklake_data_file df ON df.created_snapshot_id = s.snapshot_id
                                       AND df.table_id = tc.table_id
        GROUP BY
            dt.schema_name,
            date_trunc('hour', s.created_at)
    $$,
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## Scenario 5: Compaction Efficiency Tracking

After each compaction run, track how much space was reclaimed and how many
small files were merged.

```sql
-- Compaction events: files created vs. deleted per snapshot
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_compaction_events',
    query        => $$
        WITH deleted_in_snapshot AS (
            SELECT
                df.deleted_snapshot_id AS snapshot_id,
                dt.schema_name,
                dt.table_name,
                COUNT(*)               AS files_compacted,
                SUM(df.file_size_bytes) AS bytes_compacted
            FROM ducklake_data_file df
            JOIN ducklake_table dt ON dt.table_id = df.table_id
            WHERE df.deleted_snapshot_id IS NOT NULL
            GROUP BY df.deleted_snapshot_id, dt.schema_name, dt.table_name
        ),
        created_in_snapshot AS (
            SELECT
                df.created_snapshot_id AS snapshot_id,
                COUNT(*)               AS files_created
            FROM ducklake_data_file df
            JOIN ducklake_table dt ON dt.table_id = df.table_id
            WHERE df.created_snapshot_id IS NOT NULL
            GROUP BY df.created_snapshot_id
        )
        SELECT
            d.schema_name,
            d.table_name,
            s.created_at,
            d.files_compacted,
            c.files_created,
            d.bytes_compacted
        FROM deleted_in_snapshot d
        JOIN ducklake_snapshot s ON s.snapshot_id = d.snapshot_id
        LEFT JOIN created_in_snapshot c ON c.snapshot_id = d.snapshot_id
        WHERE d.files_compacted > c.files_created   -- net reduction = compaction
    $$,
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
```

---

## The Grafana Dashboard

Import the pre-built dashboard JSON from
[demos/ducklake-observability/grafana/](https://github.com/trickle-labs/pg-trickle/tree/main/demos/ducklake-observability/grafana/)
or create panels manually:

| Panel | Query | Alert |
|-------|-------|-------|
| Small files per table | `ducklake_small_file_counts` | > 50 files → WARN, > 200 → CRIT |
| Snapshot rate | `ducklake_snapshot_rate` | > 60/min for any table → WARN |
| Storage growth | `ducklake_storage_growth` | Projected > quota in 7 days → WARN |
| Tenant write volume | `ducklake_tenant_write_activity` | None (billing only) |
| Compaction events | `ducklake_compaction_events` | None (informational) |

---

## Initialisation Script

For a quick start, use the pre-packaged initialisation script from the
[DuckLake Observability in a Box demo](https://github.com/trickle-labs/pg-trickle/tree/main/demos/ducklake-observability/):

```bash
git clone https://github.com/trickle-labs/pg-trickle
cd pg-trickle/demos/ducklake-observability

# Edit .env with your PostgreSQL connection details
cp .env.example .env
$EDITOR .env

# Start the monitoring stack (PostgreSQL must already have pg_trickle + DuckLake)
docker compose up -d grafana

# Initialise the stream tables
psql "$DATABASE_URL" -f init_monitoring.sql
```

Five minutes from `git clone` to a production-quality Grafana observability
dashboard over your DuckLake deployment.

---

## Performance Notes

DuckLake's metadata tables are small relative to the data they describe. A
DuckLake deployment with 10,000 snapshots and 100,000 data files will have
metadata tables totalling under 50 MB. pg_trickle's differential refresh
processes only new rows since the last tick, so the per-cycle cost is
proportional to the write rate, not the catalog size.

For extremely active DuckLake deployments (thousands of commits per minute),
set the stream table schedule to `'5m'` or longer and consider adding a partial
index on `ducklake_snapshot.created_at` to speed up the time-range filters.

---

## Summary

DuckLake's metadata tables are a ready-made operational intelligence layer.
pg_trickle turns them into a live monitoring system in minutes. The combination
gives every DuckLake operator:

- **Real-time alerts** for compaction, snapshot rate spikes, and storage anomalies
- **Historical trend charts** for capacity planning
- **Per-tenant write metrics** for billing and quota enforcement
- **Zero external infrastructure** — everything runs inside the PostgreSQL instance
  that already hosts the DuckLake catalog

---

*See also:*
- [Demo: DuckLake Observability in a Box](../demos/ducklake-observability/README.md)
- [Tutorial: Real-Time Dashboards on Your Data Lake](ducklake-real-time-dashboards.md)
- [Tutorial: The Modern Data Stack in One Box](ducklake-modern-data-stack.md)
- [Blog: Why pg_trickle + DuckLake Is the Missing Piece for Lakehouse IVM](ducklake-ivm-missing-piece.md)
