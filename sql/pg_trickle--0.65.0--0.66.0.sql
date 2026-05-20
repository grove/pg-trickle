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

-- ── FUNC-1: Update create_stream_table with sink parameters ──────────────
--
-- The 0.65.0 binary registered create_stream_table with 16 parameters.
-- The 0.66.0 binary adds sink, ducklake_sink_path, ducklake_sink_table_id
-- at positions 17-19.  pgrx unboxes arguments by ordinal position, so the
-- catalog function signature MUST match the Rust function signature exactly.
-- DROP the old 16-parameter overload and replace it with the 19-parameter one.

DROP FUNCTION IF EXISTS pgtrickle."create_stream_table"(
    TEXT, TEXT, TEXT, TEXT, BOOLEAN, TEXT, TEXT, TEXT, BOOLEAN, BOOLEAN,
    TEXT, INT, FLOAT8, TEXT, BOOLEAN, TEXT
);
CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table"(
    "name"                       TEXT,
    "query"                      TEXT,
    "schedule"                   TEXT    DEFAULT 'calculated',
    "refresh_mode"               TEXT    DEFAULT 'AUTO',
    "initialize"                 BOOLEAN DEFAULT true,
    "diamond_consistency"        TEXT    DEFAULT NULL,
    "diamond_schedule_policy"    TEXT    DEFAULT NULL,
    "cdc_mode"                   TEXT    DEFAULT NULL,
    "append_only"                BOOLEAN DEFAULT false,
    "pooler_compatibility_mode"  BOOLEAN DEFAULT false,
    "partition_by"               TEXT    DEFAULT NULL,
    "max_differential_joins"     INT     DEFAULT NULL,
    "max_delta_fraction"         FLOAT8  DEFAULT NULL,
    "output_distribution_column" TEXT    DEFAULT NULL,
    "temporal"                   BOOLEAN DEFAULT false,
    "storage_backend"            TEXT    DEFAULT NULL,
    "sink"                       TEXT    DEFAULT NULL,
    "ducklake_sink_path"         TEXT    DEFAULT NULL,
    "ducklake_sink_table_id"     BIGINT  DEFAULT NULL
) RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_stream_table_wrapper';

-- ── FUNC-2: Update create_stream_table_if_not_exists with sink parameters ──

DROP FUNCTION IF EXISTS pgtrickle."create_stream_table_if_not_exists"(
    TEXT, TEXT, TEXT, TEXT, BOOLEAN, TEXT, TEXT, TEXT, BOOLEAN, BOOLEAN,
    TEXT, INT, FLOAT8, TEXT, BOOLEAN, TEXT
);
CREATE OR REPLACE FUNCTION pgtrickle."create_stream_table_if_not_exists"(
    "name"                       TEXT,
    "query"                      TEXT,
    "schedule"                   TEXT    DEFAULT 'calculated',
    "refresh_mode"               TEXT    DEFAULT 'AUTO',
    "initialize"                 BOOLEAN DEFAULT true,
    "diamond_consistency"        TEXT    DEFAULT NULL,
    "diamond_schedule_policy"    TEXT    DEFAULT NULL,
    "cdc_mode"                   TEXT    DEFAULT NULL,
    "append_only"                BOOLEAN DEFAULT false,
    "pooler_compatibility_mode"  BOOLEAN DEFAULT false,
    "partition_by"               TEXT    DEFAULT NULL,
    "max_differential_joins"     INT     DEFAULT NULL,
    "max_delta_fraction"         FLOAT8  DEFAULT NULL,
    "output_distribution_column" TEXT    DEFAULT NULL,
    "temporal"                   BOOLEAN DEFAULT false,
    "storage_backend"            TEXT    DEFAULT NULL,
    "sink"                       TEXT    DEFAULT NULL,
    "ducklake_sink_path"         TEXT    DEFAULT NULL,
    "ducklake_sink_table_id"     BIGINT  DEFAULT NULL
) RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME', 'create_stream_table_if_not_exists_wrapper';

-- ── FUNC-3: Update alter_stream_table with sink parameters ───────────────
--
-- The 0.65.0 alter_stream_table had 19 parameters (no sink columns).
-- The 0.66.0 binary adds sink, ducklake_sink_path, ducklake_sink_table_id.

DROP FUNCTION IF EXISTS pgtrickle."alter_stream_table"(
    TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, BOOLEAN, BOOLEAN,
    TEXT, TEXT, BIGINT, INT, TEXT, INT, FLOAT8, TEXT, FLOAT8
);
CREATE OR REPLACE FUNCTION pgtrickle."alter_stream_table"(
    "name"                       TEXT,
    "query"                      TEXT    DEFAULT NULL,
    "schedule"                   TEXT    DEFAULT NULL,
    "refresh_mode"               TEXT    DEFAULT NULL,
    "status"                     TEXT    DEFAULT NULL,
    "diamond_consistency"        TEXT    DEFAULT NULL,
    "diamond_schedule_policy"    TEXT    DEFAULT NULL,
    "cdc_mode"                   TEXT    DEFAULT NULL,
    "append_only"                BOOLEAN DEFAULT NULL,
    "pooler_compatibility_mode"  BOOLEAN DEFAULT NULL,
    "tier"                       TEXT    DEFAULT NULL,
    "fuse"                       TEXT    DEFAULT NULL,
    "fuse_ceiling"               BIGINT  DEFAULT NULL,
    "fuse_sensitivity"           INT     DEFAULT NULL,
    "partition_by"               TEXT    DEFAULT NULL,
    "max_differential_joins"     INT     DEFAULT NULL,
    "max_delta_fraction"         FLOAT8  DEFAULT NULL,
    "post_refresh_action"        TEXT    DEFAULT NULL,
    "reindex_drift_threshold"    FLOAT8  DEFAULT NULL,
    "sink"                       TEXT    DEFAULT NULL,
    "ducklake_sink_path"         TEXT    DEFAULT NULL,
    "ducklake_sink_table_id"     BIGINT  DEFAULT NULL
) RETURNS void
LANGUAGE c
AS 'MODULE_PATHNAME', 'alter_stream_table_wrapper';
