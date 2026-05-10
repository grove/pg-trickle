//! Citus module: `pgt_st_locks` lease acquisition loop and distributed CDC polling.
//!
//! Handles the COORD-10 through COORD-14 distributed CDC orchestration for
//! Citus-partitioned source tables.

use std::cell::RefCell;
use std::collections::HashMap;

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::config;

// ── COORD-14 (v0.34.0): Per-worker consecutive failure counts ─────────────
//
// Key: (pgt_id, "worker_name:port")
// Value: number of consecutive poll failures for that worker.
//
// Reset to 0 on each successful poll; incremented on each failure.
// When the count reaches `citus_worker_retry_ticks`, a warning is logged.
thread_local! {
    pub(super) static CITUS_WORKER_FAILURES: RefCell<HashMap<(i64, String), i32>> =
        RefCell::new(HashMap::new());
}

/// Drive distributed CDC for a single stream table.
///
/// Called from `refresh_single_st()` for stream tables that have distributed
/// (Citus) source tables. Handles:
/// - COORD-12: pgt_st_locks lease acquisition
/// - COORD-10: Polling each worker slot for changes
/// - COORD-11: Ensuring each worker slot exists
/// - COORD-13: Topology change detection and recovery
/// - COORD-14: Worker failure tracking and alerting
///
/// Returns the lock key that was acquired (if any), so the caller can release
/// it after the local refresh completes.
pub(super) fn drive_distributed_cdc(pgt_id: i64) -> Option<String> {
    if !crate::citus::is_citus_loaded() {
        return None;
    }

    // Check whether this ST has any distributed sources.
    let deps = match crate::catalog::StDependency::get_for_st(pgt_id) {
        Ok(d) => d,
        Err(e) => {
            pgrx::debug1!(
                "[pg_trickle] drive_distributed_cdc: failed to load deps for pgt_id={}: {}",
                pgt_id,
                e
            );
            return None;
        }
    };

    let has_distributed = deps.iter().any(|d| d.source_placement == "distributed");
    if !has_distributed {
        return None;
    }

    // COORD-13: Detect topology changes.
    if crate::citus::detect_topology_change(pgt_id) {
        pgrx::log!(
            "[pg_trickle] COORD-13: topology change detected for pgt_id={} — reconciling worker slots",
            pgt_id
        );
        match crate::citus::reconcile_worker_slots(pgt_id) {
            Ok(n) => {
                pgrx::log!(
                    "[pg_trickle] COORD-13: reconciled {} worker slot entries for pgt_id={}",
                    n,
                    pgt_id
                );
            }
            Err(e) => {
                pgrx::warning!(
                    "[pg_trickle] COORD-13: reconcile_worker_slots failed for pgt_id={}: {}",
                    pgt_id,
                    e
                );
            }
        }
        // Mark the ST for a full refresh to catch up after topology change.
        let _ = StreamTableMeta::mark_for_reinitialize(pgt_id);
        // Return without polling: the FULL refresh on this tick will catch up.
        return None;
    }

    // COORD-12: Acquire pgt_st_locks lease.
    let lease_ms = config::pg_trickle_citus_st_lock_lease_ms();
    let lock_key = format!("pgt_{pgt_id}");
    // Use the local backend PID as the holder so we can recognize our own locks.
    let holder = format!("scheduler_{}", unsafe { pg_sys::MyProcPid });

    if lease_ms > 0 {
        match crate::citus::try_acquire_st_lock(&lock_key, &holder, lease_ms) {
            Ok(true) => {
                pgrx::debug1!(
                    "[pg_trickle] COORD-12: acquired pgt_st_locks '{}' ({}ms)",
                    lock_key,
                    lease_ms
                );
            }
            Ok(false) => {
                pgrx::debug1!(
                    "[pg_trickle] COORD-12: pgt_st_locks '{}' held by another coordinator — skipping",
                    lock_key
                );
                return None;
            }
            Err(e) => {
                pgrx::warning!(
                    "[pg_trickle] COORD-12: try_acquire_st_lock failed for '{}': {}",
                    lock_key,
                    e
                );
                return None;
            }
        }
    }

    // Load worker slots.
    let worker_slots = crate::citus::get_worker_slots_for_st(pgt_id);
    if worker_slots.is_empty() {
        // No slots registered yet; reconcile on next tick.
        if lease_ms > 0 {
            let _ = crate::citus::release_st_lock(&lock_key, &holder);
        }
        return None;
    }

    let retry_threshold = config::pg_trickle_citus_worker_retry_ticks();

    for slot in &worker_slots {
        let worker = slot.node_addr();
        let worker_key = format!("{}:{}", slot.worker_name, slot.worker_port);

        // COORD-11: Ensure the slot exists on the worker.
        if let Err(e) = crate::citus::ensure_worker_slot(&worker, &slot.slot_name) {
            pgrx::warning!(
                "[pg_trickle] COORD-11: ensure_worker_slot failed for {} slot '{}': {}",
                worker_key,
                slot.slot_name,
                e
            );
            // Track failure and continue to next worker.
            record_worker_failure(pgt_id, &worker_key, retry_threshold);
            continue;
        }

        // COORD-10: Build source descriptor and poll.
        // Resolve pk_columns and all columns needed for the change buffer.
        let pk_columns = match crate::cdc::resolve_pk_columns(slot.source_relid) {
            Ok(pk) => pk,
            Err(e) => {
                pgrx::warning!(
                    "[pg_trickle] COORD-10: resolve_pk_columns for oid={} failed: {}",
                    slot.source_relid.to_u32(),
                    e
                );
                record_worker_failure(pgt_id, &worker_key, retry_threshold);
                continue;
            }
        };

        let columns = match crate::cdc::resolve_source_column_defs(slot.source_relid) {
            Ok(c) => c,
            Err(e) => {
                pgrx::warning!(
                    "[pg_trickle] COORD-10: resolve_source_column_defs for oid={} failed: {}",
                    slot.source_relid.to_u32(),
                    e
                );
                record_worker_failure(pgt_id, &worker_key, retry_threshold);
                continue;
            }
        };

        // Resolve source schema.table name.
        let source_qualified_table = crate::citus::stable_name_for_oid(slot.source_relid)
            .ok()
            .and_then(|_| {
                // Build qualified name from catalog.
                pgrx::Spi::get_one_with_args::<String>(
                    "SELECT n.nspname || '.' || c.relname \
                     FROM pg_class c \
                     JOIN pg_namespace n ON n.oid = c.relnamespace \
                     WHERE c.oid = $1",
                    &[slot.source_relid.into()],
                )
                .ok()
                .flatten()
            })
            .unwrap_or_else(|| slot.source_relid.to_u32().to_string());

        let change_schema = "pgtrickle_changes";
        let max_changes: i64 = 10_000;

        let src = crate::citus::WorkerPollSource {
            change_schema,
            source_qualified_table: &source_qualified_table,
            source_oid: slot.source_relid,
            pk_columns: &pk_columns,
            columns: &columns,
        };

        match crate::citus::poll_worker_slot_changes(&worker, &slot.slot_name, max_changes, &src) {
            Ok(n_changes) => {
                pgrx::debug1!(
                    "[pg_trickle] COORD-10: polled {} changes from worker {} slot '{}'",
                    n_changes,
                    worker_key,
                    slot.slot_name
                );
                // COORD-14: Reset failure counter on success.
                reset_worker_failure(pgt_id, &worker_key);
            }
            Err(e) => {
                pgrx::warning!(
                    "[pg_trickle] COORD-10: poll_worker_slot_changes failed for worker {} slot '{}': {}",
                    worker_key,
                    slot.slot_name,
                    e
                );
                record_worker_failure(pgt_id, &worker_key, retry_threshold);
            }
        }
    }

    // Return the lock key so the caller can release it after local refresh.
    if lease_ms > 0 { Some(lock_key) } else { None }
}

/// COORD-14: Increment the consecutive failure counter for a worker and emit
/// an operator warning if the threshold is reached.
pub(super) fn record_worker_failure(pgt_id: i64, worker_key: &str, threshold: i32) {
    CITUS_WORKER_FAILURES.with(|m| {
        let mut binding = m.borrow_mut();
        let count = binding
            .entry((pgt_id, worker_key.to_string()))
            .or_insert(0);
        *count += 1;
        let current = *count;
        if threshold > 0 && current >= threshold {
            pgrx::warning!(
                "[pg_trickle] COORD-14: worker {} has failed {} consecutive times for pgt_id={} \
                 — stream table flagged in citus_status. Refreshes continue against healthy workers.",
                worker_key,
                current,
                pgt_id
            );
            // A46-11: Persist the threshold-crossing to shared memory so it
            // survives scheduler restarts and is visible through preflight().
            crate::shmem::increment_citus_worker_failure_total();
        }
    });
}

/// COORD-14: Reset the consecutive failure counter for a worker after a
/// successful poll.
pub(super) fn reset_worker_failure(pgt_id: i64, worker_key: &str) {
    CITUS_WORKER_FAILURES.with(|m| {
        m.borrow_mut().remove(&(pgt_id, worker_key.to_string()));
    });
}

/// Read the current failure count for a (pgt_id, worker_key) pair.
///
/// Returns 0 when no entry exists.  Used in unit tests to inspect
/// the thread-local map without triggering pgrx warnings.
#[cfg(test)]
fn worker_failure_count(pgt_id: i64, worker_key: &str) -> i32 {
    CITUS_WORKER_FAILURES.with(|m| {
        *m.borrow()
            .get(&(pgt_id, worker_key.to_string()))
            .unwrap_or(&0)
    })
}

// ── Unit tests ─────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    // citus.rs orchestrates distributed CDC via SPI calls.  The
    // COORD-14 failure tracking functions manipulate a thread-local map
    // without touching SPI, so they are fully testable here.  All other
    // functions are covered by E2E tests in tests/e2e_citus_*.rs.

    /// Clear the thread-local map between tests to prevent state leakage.
    fn clear_failures() {
        CITUS_WORKER_FAILURES.with(|m| m.borrow_mut().clear());
    }

    #[test]
    fn test_record_worker_failure_increments_count() {
        clear_failures();
        // threshold=0 avoids pgrx::warning! and shmem::increment_*
        record_worker_failure(1, "worker-a:5432", 0);
        assert_eq!(worker_failure_count(1, "worker-a:5432"), 1);
        record_worker_failure(1, "worker-a:5432", 0);
        assert_eq!(worker_failure_count(1, "worker-a:5432"), 2);
        clear_failures();
    }

    #[test]
    fn test_record_worker_failure_separate_pgt_ids() {
        clear_failures();
        record_worker_failure(10, "worker-a:5432", 0);
        record_worker_failure(20, "worker-a:5432", 0);
        assert_eq!(worker_failure_count(10, "worker-a:5432"), 1);
        assert_eq!(worker_failure_count(20, "worker-a:5432"), 1);
        clear_failures();
    }

    #[test]
    fn test_record_worker_failure_separate_worker_keys() {
        clear_failures();
        record_worker_failure(1, "worker-a:5432", 0);
        record_worker_failure(1, "worker-b:5433", 0);
        assert_eq!(worker_failure_count(1, "worker-a:5432"), 1);
        assert_eq!(worker_failure_count(1, "worker-b:5433"), 1);
        clear_failures();
    }

    #[test]
    fn test_reset_worker_failure_clears_counter() {
        clear_failures();
        record_worker_failure(1, "worker-a:5432", 0);
        record_worker_failure(1, "worker-a:5432", 0);
        assert_eq!(worker_failure_count(1, "worker-a:5432"), 2);

        reset_worker_failure(1, "worker-a:5432");
        assert_eq!(worker_failure_count(1, "worker-a:5432"), 0);
        clear_failures();
    }

    #[test]
    fn test_reset_worker_failure_missing_entry_is_noop() {
        clear_failures();
        // Resetting a non-existent entry should not panic.
        reset_worker_failure(999, "nonexistent:5432");
        assert_eq!(worker_failure_count(999, "nonexistent:5432"), 0);
    }

    #[test]
    fn test_reset_worker_failure_does_not_affect_other_keys() {
        clear_failures();
        record_worker_failure(1, "worker-a:5432", 0);
        record_worker_failure(1, "worker-b:5433", 0);

        reset_worker_failure(1, "worker-a:5432");
        assert_eq!(worker_failure_count(1, "worker-a:5432"), 0);
        assert_eq!(worker_failure_count(1, "worker-b:5433"), 1);
        clear_failures();
    }
}
