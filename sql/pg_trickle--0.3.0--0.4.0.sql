-- pg_trickle 0.3.0 -> 0.4.0 upgrade script
-- CSS1: LSN tick watermark column for cross-source snapshot consistency.
ALTER TABLE pgtrickle.pgt_refresh_history
    ADD COLUMN IF NOT EXISTS tick_watermark_lsn PG_LSN;

-- B2: Statement-level CDC trigger migration.
-- Declare the new rebuild helper (compiled into the 0.4.0 .so) and run it
-- once to migrate all existing row-level CDC triggers to statement-level.
-- The function is retained as pgtrickle.rebuild_cdc_triggers() for manual use
-- (e.g. after changing pg_trickle.cdc_trigger_mode).
CREATE OR REPLACE FUNCTION pgtrickle.rebuild_cdc_triggers()
    RETURNS text
    STRICT
    LANGUAGE c /* Rust */
    AS 'MODULE_PATHNAME', 'rebuild_cdc_triggers_wrapper';

SELECT pgtrickle.rebuild_cdc_triggers();
