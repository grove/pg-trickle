-- pg_trickle 0.22.0 → 0.23.0 upgrade migration
-- ============================================
--
-- v0.23.0 — TPC-H DVM Scaling Performance
--
-- P1-2: log_delta_sql GUC (config-only, no schema change)
-- P5-1: delta_work_mem GUC (config-only, no schema change)
-- P5-2: delta_enable_nestloop GUC (config-only, no schema change)
-- PERF-5: analyze_before_delta GUC (config-only, no schema change)
-- SCAL-2: max_change_buffer_alert_rows GUC (config-only, no schema change)
-- UX-7: diff_output_format GUC (config-only, no schema change)
-- UX-3: explain_diff_sql() SQL function (Rust-defined via pgrx)
-- STAB-4: pgtrickle_refresh_stats() SQL function (Rust-defined via pgrx)
--
-- No schema changes are required for this version. All new SQL functions
-- are defined in Rust via #[pg_extern] and are automatically available
-- after the shared library is loaded. The GUCs are compile-time settings
-- registered in _PG_init().
--
-- This migration file exists for the upgrade chain and version tracking.

-- Record the schema version for the upgrade chain.
INSERT INTO pgtrickle.pgt_schema_version (version, description, installed_at)
VALUES ('0.23.0', 'TPC-H DVM Scaling Performance', now())
ON CONFLICT (version) DO NOTHING;
