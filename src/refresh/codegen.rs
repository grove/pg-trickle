// ARCH-1: SQL code-generation sub-module for the refresh pipeline.
//
// This module will contain all SQL template builders used by the refresh
// executor:
// - `build_merge_sql`
// - `build_trigger_*_sql`
// - `build_weight_agg_using` / `build_keyless_weight_agg`
// - `build_is_distinct_clause`
// - `prewarm_merge_cache`
// - `format_col_list`, `format_prefixed_col_list`, `format_update_set`
// - `MERGE SQL template cache` (CachedMergeTemplate)
//
// Currently all code lives in `super` (mod.rs).  This file is the landing
// zone for the SQL-generation layer during the ongoing ARCH-1 migration.
