-- pg_trickle 0.64.0 -> 0.65.0 upgrade migration
--
-- v0.65.0 — DuckLake Phase 2: Change-Feed CDC Adapter
--
-- Changes in this release:
--
--   CDC-6: DuckLake change-feed CDC adapter
--     - New CdcMode::DuckLakeChangeFeed variant — stream tables backed by
--       DuckLake foreign tables can use table_changes() for differential
--       refresh instead of WAL decoding.
--
--   CDC-7: Snapshot-based frontier model
--     - SourceVersion gains snapshot_id: Option<i64> for tracking the last
--       DuckLake snapshot consumed. Backwards-compatible (serde default).
--
--   CDC-8: Inlined-data trigger adapter
--     - DuckLake inlined-data tables (small datasets stored in the catalog)
--       now get AFTER triggers installed automatically, enabling differential
--       refresh of stream tables that depend on them.
--
--   ROW-5: ExternalStableId row-ID strategy
--     - New RowIdStrategy::ExternalStableId { id_column } for sources that
--       expose a stable row ID column (e.g. DuckLake's rowid).
--
--   CONF-3: ducklake_compaction_policy GUC
--     - New pg_trickle.ducklake_compaction_policy = 'fallback' | 'error'
--       controls what happens when a snapshot has been compacted away.
--
--   SCHEMA-1: ducklake_compaction_policy column on pgt_stream_tables
--     - Per-table override for the global compaction policy GUC.
--
--   SCHEMA-2: DUCKLAKE_CHANGE_FEED added to pgt_dependencies.cdc_mode CHECK
--     - Extends the allowed cdc_mode values.

-- ── SCHEMA-1: Add ducklake_compaction_policy to pgt_stream_tables ────────

ALTER TABLE pgtrickle.pgt_stream_tables
    ADD COLUMN IF NOT EXISTS ducklake_compaction_policy TEXT DEFAULT NULL
    CHECK (
        ducklake_compaction_policy IS NULL
        OR ducklake_compaction_policy IN ('fallback', 'error')
    );

COMMENT ON COLUMN pgtrickle.pgt_stream_tables.ducklake_compaction_policy IS
    'Per-table DuckLake compaction policy override. NULL = use global GUC '
    'pg_trickle.ducklake_compaction_policy. ''fallback'' = full refresh on '
    'snapshot expiry; ''error'' = raise error and halt.';

-- ── SCHEMA-2: Add DUCKLAKE_CHANGE_FEED to cdc_mode CHECK constraint ──────
--
-- The CHECK constraint on pgt_dependencies.cdc_mode is auto-named by
-- PostgreSQL (typically pgt_dependencies_cdc_mode_check). We drop it and
-- recreate it with the new allowed value.

DO $$
DECLARE
    v_constraint text;
BEGIN
    SELECT tc.constraint_name
      INTO v_constraint
      FROM information_schema.table_constraints tc
     WHERE tc.table_schema = 'pgtrickle'
       AND tc.table_name   = 'pgt_dependencies'
       AND tc.constraint_type = 'CHECK'
       AND tc.constraint_name LIKE '%cdc_mode%'
     LIMIT 1;

    IF v_constraint IS NOT NULL THEN
        EXECUTE format(
            'ALTER TABLE pgtrickle.pgt_dependencies DROP CONSTRAINT %I',
            v_constraint
        );
    END IF;
END;
$$;

ALTER TABLE pgtrickle.pgt_dependencies
    ADD CONSTRAINT pgt_dependencies_cdc_mode_check
    CHECK (cdc_mode IN ('TRIGGER', 'TRANSITIONING', 'WAL', 'DUCKLAKE_CHANGE_FEED'));

COMMENT ON COLUMN pgtrickle.pgt_dependencies.cdc_mode IS
    'CDC mode for this dependency: TRIGGER, TRANSITIONING, WAL, or DUCKLAKE_CHANGE_FEED.';
