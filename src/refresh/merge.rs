// ARCH-1: MERGE execution sub-module for the refresh pipeline.
//
// This module will contain the differential MERGE execution path:
// - `execute_differential_refresh` (currently around line 3516 in mod.rs)
// - `execute_full_refresh`
// - `execute_topk_refresh`
// - `execute_no_data_refresh`
// - `execute_incremental_truncate_delete`
// - Partition-aware MERGE helpers (`execute_hash_partitioned_merge`, etc.)
// - Delta amplification detection
//
// Currently all code lives in `super` (mod.rs).  This file is the landing
// zone for the MERGE execution layer during the ongoing ARCH-1 migration.
