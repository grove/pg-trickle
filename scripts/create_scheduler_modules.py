#!/usr/bin/env python3
"""CQ-10-01: Create scheduler submodules from mod.rs extraction."""

import os

BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
MOD_RS = os.path.join(BASE, 'src', 'scheduler', 'mod.rs')

with open(MOD_RS) as f:
    lines = f.read().split('\n')

# ── dispatch.rs ────────────────────────────────────────────────────────────
dispatch_p1 = '\n'.join(lines[473:791])   # lines 474–791 (0-indexed 473–790)
dispatch_p2 = '\n'.join(lines[1670:2371]) # lines 1671–2371 (0-indexed 1670–2370)

dispatch_header = """\
//! CQ-10-01 (v0.49.0): Dynamic refresh worker spawn, parallel dispatch state.
//!
//! Extracted from `scheduler/mod.rs` as part of the scheduler decomposition.
//! Contains:
//!   - `spawn_refresh_worker` — spawn a dynamic per-job BGW
//!   - `pg_trickle_refresh_worker_main` — BGW entry point
//!   - `extract_panic_message`, `parse_worker_extra` — pure helpers
//!   - Parallel dispatch state structs and `parallel_dispatch_tick`
//!   - `reap_dead_worker_jobs`, `reconcile_parallel_state`

use std::collections::HashMap;
use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::*;
use pgrx::prelude::*;

use crate::catalog::{JobStatus, SchedulerJob, StreamTableMeta};
use crate::config;
use crate::dag::{
    DiamondConsistency, DiamondSchedulePolicy, ExecutionUnit, ExecutionUnitDag, ExecutionUnitId,
    NodeId, RefreshMode, Scc, StDag, StStatus,
};
use crate::error::{classify_error_for_retry, RetryPolicy, RetryState};
use crate::shmem;

// Private items from the parent module (scheduler/mod.rs) are accessible
// here because dispatch is a child module of scheduler.
use super::{
    check_schedule, check_skip_needed, compute_freshness_deadline, count_pending_changes,
    emit_frozen_tier_skip, emit_stale_alert_if_needed, evaluate_fuse,
    execute_worker_atomic_group, execute_worker_cyclic_scc, execute_worker_fused_chain,
    execute_worker_immediate_closure, execute_worker_singleton, load_st_by_id,
    update_backoff_factor,
};
use super::cost::compute_per_db_quota;
use super::watermark::compute_worker_tick_watermark;

"""

dispatch_content = dispatch_header + dispatch_p1 + '\n\n' + dispatch_p2
dispatch_path = os.path.join(BASE, 'src', 'scheduler', 'dispatch.rs')
with open(dispatch_path, 'w') as f:
    f.write(dispatch_content)
print(f"Created dispatch.rs: {len(dispatch_content.splitlines())} lines")

# ── scheduler_loop.rs ──────────────────────────────────────────────────────
loop_p1 = '\n'.join(lines[222:472])    # lines 223–472 (0-indexed 222–471)
loop_p2 = '\n'.join(lines[2372:3611]) # lines 2373–3611 (0-indexed 2372–3610)

loop_header = """\
//! CQ-10-01 (v0.49.0): Scheduler main loops and BGW registration.
//!
//! Extracted from `scheduler/mod.rs` as part of the scheduler decomposition.
//! Contains:
//!   - `register_launcher_worker` — static BGW registration
//!   - `pg_trickle_launcher_main` — launcher BGW entry point
//!   - `register_scheduler_worker` — per-DB BGW registration
//!   - `pg_trickle_scheduler_main` — per-DB scheduler BGW entry point

use std::collections::{HashMap, HashSet};
use std::panic::AssertUnwindSafe;

use pgrx::bgworkers::*;
use pgrx::prelude::*;

use crate::catalog::{JobStatus, RefreshRecord, SchedulerJob, StreamTableMeta};
use crate::cdc;
use crate::config;
use crate::dag::{
    DiamondConsistency, DiamondSchedulePolicy, ExecutionUnit, ExecutionUnitDag, ExecutionUnitId,
    NodeId, RefreshMode, Scc, StDag, StStatus,
};
use crate::error::{RetryPolicy, RetryState};
use crate::monitor;
use crate::refresh::{self, RefreshAction};
use crate::shmem;
use crate::version;
use crate::wal_decoder;

// Private items from the parent module accessible in child modules.
use super::{
    batched_has_source_changes, check_cdc_transition_health, check_extension_version_match,
    check_unlogged_buffer_crash_recovery, current_epoch_ms, execute_post_refresh_action,
    execute_scheduled_refresh, get_cached_active_stream_tables, group_schedule_policy,
    has_l0_cache_entry, invalidate_catalog_snapshot_cache, is_any_source_gated, is_falling_behind,
    is_group_due, is_watermark_misaligned, is_watermark_stuck, load_gated_source_oids,
    log_gated_skip, log_watermark_skip, rebuild_dag_copy_on_write, recover_from_crash,
    refresh_single_st, self_monitoring_anomaly_notify, self_monitoring_auto_apply_tick,
    sla_tier_adjustment_tick,
};
use super::dispatch::{parallel_dispatch_tick, reconcile_parallel_state, ParallelDispatchState};
use super::watermark::compute_coordinator_tick_watermark;

"""

loop_content = loop_header + loop_p1 + '\n\n' + loop_p2
loop_path = os.path.join(BASE, 'src', 'scheduler', 'scheduler_loop.rs')
with open(loop_path, 'w') as f:
    f.write(loop_content)
print(f"Created scheduler_loop.rs: {len(loop_content.splitlines())} lines")
