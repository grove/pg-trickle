-- pg_trickle 0.65.0 -> 0.66.0 upgrade migration
--
-- v0.66.0 — DuckLake Phase 3a: Parquet Sink Infrastructure
--
-- Changes in this release:
--
--   F-4: Parquet delta export via arrow-rs
--     - New Rust module `ducklake_sink` that serialises each refresh-cycle
--       delta to a Parquet file using the arrow-rs / parquet crates.
--
--   F-2: DuckLake sink output mode (`sink => 'ducklake'`)
--     - New catalog column `ducklake_sink_mode` on pgt_stream_tables.
--     - New catalog column `ducklake_sink_path` on pgt_stream_tables.
--     - New catalog column `ducklake_sink_table_id` on pgt_stream_tables.
--     - New API parameters `sink` and `ducklake_sink_path` on
--       pgtrickle.create_stream_table() and pgtrickle.alter_stream_table().
--     - After each successful refresh the scheduler calls
--       `run_ducklake_sink()`, which writes a Parquet delta and registers
--       the file in the DuckLake catalog.
--
--   S3 / object-store upload integration
--     - New GUC `pg_trickle.ducklake_sink_s3_endpoint` (default '').
--     - New GUC `pg_trickle.ducklake_sink_s3_region` (default 'us-east-1').
--     - New GUC `pg_trickle.ducklake_sink_compression` (default 'snappy').
--
--   DuckLake catalog transaction writer
--     - Inserts into `ducklake_data_file`, updates `ducklake_table_stats`,
--       inserts `ducklake_snapshot`; rolls back cleanly on S3 failure.
--
--   F-9: Encryption key pass-through
--     - When the target DuckLake table has encryption enabled, the writer
--       generates a fresh per-file key, stores it in `ducklake_data_file`,
--       and applies it during Parquet serialisation.
--     - New GUC `pg_trickle.ducklake_sink_encryption_key_prefix` for the
--       key-naming scheme.

-- ── SCHEMA-1: Add ducklake_sink_mode to pgt_stream_tables ────────────────

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS ducklake_sink_mode TEXT DEFAULT NULL
    CHECK (
        ducklake_sink_mode IS NULL
        OR ducklake_sink_mode IN ('append', 'replace')
    );

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.ducklake_sink_mode IS
    'DuckLake sink output mode. NULL = no sink. ''append'' = write each '
    'differential delta as a new Parquet file (accumulates). '
    '''replace'' = overwrite the full result on every refresh cycle.';

-- ── SCHEMA-2: Add ducklake_sink_path to pgt_stream_tables ────────────────

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS ducklake_sink_path TEXT DEFAULT NULL;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.ducklake_sink_path IS
    'Object-store path where Parquet files are written. '
    'Supports s3://, gs://, az://, and file:// schemes. '
    'Required when ducklake_sink_mode IS NOT NULL.';

-- ── SCHEMA-3: Add ducklake_sink_table_id to pgt_stream_tables ────────────

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS ducklake_sink_table_id BIGINT DEFAULT NULL;

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.ducklake_sink_table_id IS
    'DuckLake table_id from ducklake_table. When set, the sink writer '
    'registers each new Parquet file in the DuckLake catalog under this '
    'table. NULL means no DuckLake catalog registration is performed '
    '(files are written to object storage only).';
