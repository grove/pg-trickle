-- DuckLake Observability Stream Tables
-- Run this script against your DuckLake PostgreSQL catalog database
-- to install the monitoring stream tables.
--
-- Prerequisites:
--   CREATE EXTENSION pg_trickle;  (if not already installed)
--
-- Usage:
--   psql "$DATABASE_URL" -f init_monitoring.sql

-- Ensure pg_trickle is available
CREATE EXTENSION IF NOT EXISTS pg_trickle;

-- ─────────────────────────────────────────────────────────────────────────────
-- ST-1: Small-file count per table
-- Alert threshold: small_file_count > 50 (WARNING), > 200 (CRITICAL)
-- ─────────────────────────────────────────────────────────────────────────────
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
        WHERE df.file_size_bytes < 10 * 1024 * 1024
          AND df.deleted_snapshot_id IS NULL
        GROUP BY dt.schema_name, dt.table_name
    $$,
    schedule     => '1m',
    refresh_mode => 'DIFFERENTIAL'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- ST-2: Snapshot creation rate per table per minute
-- Alert threshold: > 60 commits/minute for any single table
-- ─────────────────────────────────────────────────────────────────────────────
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_snapshot_rate',
    query        => $$
        SELECT
            date_trunc('minute', s.created_at)   AS minute,
            dt.schema_name,
            dt.table_name,
            COUNT(DISTINCT s.snapshot_id)         AS snapshots_in_minute,
            SUM(tc.rows_added)                    AS rows_added,
            SUM(tc.rows_deleted)                  AS rows_deleted
        FROM ducklake_snapshot s
        JOIN ducklake_table_changes tc ON tc.snapshot_id = s.snapshot_id
        JOIN ducklake_table dt         ON dt.table_id = tc.table_id
        GROUP BY
            date_trunc('minute', s.created_at),
            dt.schema_name,
            dt.table_name
    $$,
    schedule     => '30s',
    refresh_mode => 'DIFFERENTIAL'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- ST-3: Storage in use per table (active files only)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_storage_growth',
    query        => $$
        SELECT
            dt.schema_name,
            dt.table_name,
            SUM(df.file_size_bytes)  AS active_bytes,
            COUNT(*)                 AS active_file_count,
            SUM(df.row_count)        AS active_row_count
        FROM ducklake_data_file df
        JOIN ducklake_table dt ON dt.table_id = df.table_id
        WHERE df.deleted_snapshot_id IS NULL
        GROUP BY dt.schema_name, dt.table_name
    $$,
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);

-- ─────────────────────────────────────────────────────────────────────────────
-- ST-4: Per-tenant (per-schema) write activity per hour
-- ─────────────────────────────────────────────────────────────────────────────
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_tenant_activity',
    query        => $$
        SELECT
            dt.schema_name                           AS tenant_schema,
            date_trunc('hour', s.created_at)         AS hour,
            SUM(tc.rows_added)                       AS rows_added,
            SUM(tc.rows_deleted)                     AS rows_deleted,
            COUNT(DISTINCT s.snapshot_id)             AS commit_count,
            SUM(COALESCE(df.file_size_bytes, 0))     AS bytes_written
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

-- ─────────────────────────────────────────────────────────────────────────────
-- ST-5: Compaction events (snapshots where more files were deleted than created)
-- ─────────────────────────────────────────────────────────────────────────────
SELECT pgtrickle.create_stream_table(
    name         => 'ducklake_compaction_events',
    query        => $$
        WITH deleted_in_snapshot AS (
            SELECT
                df.deleted_snapshot_id  AS snapshot_id,
                dt.schema_name,
                dt.table_name,
                COUNT(*)                AS files_compacted,
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
            WHERE df.created_snapshot_id IS NOT NULL
            GROUP BY df.created_snapshot_id
        )
        SELECT
            d.schema_name,
            d.table_name,
            s.created_at          AS compacted_at,
            d.files_compacted,
            COALESCE(c.files_created, 0) AS files_created,
            d.bytes_compacted
        FROM deleted_in_snapshot d
        JOIN ducklake_snapshot s ON s.snapshot_id = d.snapshot_id
        LEFT JOIN created_in_snapshot c ON c.snapshot_id = d.snapshot_id
        WHERE d.files_compacted > COALESCE(c.files_created, 0)
    $$,
    schedule     => '5m',
    refresh_mode => 'DIFFERENTIAL'
);
