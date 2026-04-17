// ARCH-1: Orchestration sub-module for the refresh pipeline.
//
// This module will contain the top-level refresh orchestration logic:
// - `determine_refresh_action`
// - bgworker scheduling hooks
// - adaptive mode switching
// - `execute_reinitialize_refresh`
//
// Currently all code lives in `super` (mod.rs).  This file is the landing
// zone for the orchestration layer during the ongoing ARCH-1 migration.
