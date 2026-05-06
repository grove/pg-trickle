//! CQ-10-01 (v0.49.0): Frontier holdback tick watermark helpers.
//!
//! Extracted from scheduler/mod.rs as part of scheduler module decomposition.
//! Contains tick watermark computation, xmin holdback, and frontier advance logic.
//!
//! All functions here are accessible to sibling scheduler submodules via
//! `use super::watermark::*` because child modules inherit access to parent
//! private items.

use pgrx::prelude::*;

use crate::{cdc, config, shmem, version};

// ── #536: Frontier holdback tick watermark helpers ─────────────────────────

/// Unix-epoch timestamp of the last holdback-active WARNING, used to
/// rate-limit warnings to at most one per minute.
static LAST_HOLDBACK_WARN_SECS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Compute the tick watermark for the **coordinator** (main scheduler loop).
///
/// Applies the `frontier_holdback_mode` GUC logic:
/// - `"none"` / watermark disabled: use raw `pg_current_wal_lsn()`.
/// - `"xmin"`: probe `pg_stat_activity` + `pg_prepared_xacts` and hold back
///   if a long-running transaction would cause data loss.
/// - `"lsn:<N>"`: hold back by exactly N bytes.
///
/// Side effects (when holdback fires):
/// - Updates `shmem::last_tick_oldest_xmin` for the next tick.
/// - Updates `shmem::last_tick_safe_lsn_u64` for dynamic workers.
/// - Updates the holdback gauge metrics.
/// - Emits a WARNING when holdback age exceeds the warn threshold.
///
/// # Arguments
/// - `prev_watermark_lsn`: the safe LSN from the previous tick, if any.
///
/// # Returns
/// `(tick_watermark, current_oldest_xmin, oldest_txn_age_secs)`
pub(super) fn compute_coordinator_tick_watermark(
    prev_watermark_lsn: Option<&str>,
) -> (Option<String>, u64, u64) {
    if !config::pg_trickle_tick_watermark_enabled() {
        return (None, 0, 0);
    }

    let mode = config::pg_trickle_frontier_holdback_mode();

    match mode {
        config::FrontierHoldbackMode::None => {
            let lsn = match Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text") {
                Ok(v) => v,
                Err(e) => {
                    pgrx::warning!(
                        "pg_trickle scheduler: failed to fetch pg_current_wal_lsn (mode=None): {}",
                        e
                    );
                    None
                }
            };
            // Store raw write LSN for workers.
            if let Some(ref l) = lsn {
                shmem::set_last_tick_safe_lsn(version::lsn_to_u64(l));
            }
            shmem::update_holdback_metrics(0, 0);
            (lsn, 0, 0)
        }

        config::FrontierHoldbackMode::Xmin | config::FrontierHoldbackMode::InvalidLsn => {
            // Skip the probe when CDC mode is WAL -- commit-LSN ordering
            // is already safe in logical-replication mode.
            if config::pg_trickle_cdc_mode() == "wal" {
                let lsn = match Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text") {
                    Ok(v) => v,
                    Err(e) => {
                        pgrx::warning!(
                            "pg_trickle scheduler: failed to fetch pg_current_wal_lsn (mode=wal): {}",
                            e
                        );
                        None
                    }
                };
                if let Some(ref l) = lsn {
                    shmem::set_last_tick_safe_lsn(version::lsn_to_u64(l));
                }
                shmem::update_holdback_metrics(0, 0);
                return (lsn, 0, 0);
            }

            let prev_oldest_xmin = shmem::last_tick_oldest_xmin();

            match cdc::compute_safe_upper_bound(prev_watermark_lsn, prev_oldest_xmin) {
                Ok((safe_lsn, write_lsn, current_oldest_xmin, age_secs)) => {
                    // Persist for next tick and for dynamic workers under a
                    // single lock so workers never see xmin/LSN out of sync.
                    let safe_u64 = version::lsn_to_u64(&safe_lsn);
                    shmem::set_last_tick_holdback_state(current_oldest_xmin, safe_u64);

                    // Update holdback gauge metrics.
                    let write_u64 = version::lsn_to_u64(&write_lsn);
                    let holdback_bytes = write_u64.saturating_sub(safe_u64);
                    shmem::update_holdback_metrics(holdback_bytes, age_secs);

                    // Warn when holdback has been active longer than the threshold.
                    if holdback_bytes > 0 {
                        emit_holdback_warning_if_needed(age_secs);
                    }

                    (Some(safe_lsn), current_oldest_xmin, age_secs)
                }
                Err(e) => {
                    // On probe failure, hold at the previous watermark (if known)
                    // rather than advancing to the raw write LSN.  Advancing on
                    // failure is the exact unsafe behaviour the holdback is meant
                    // to prevent — the probe may have failed precisely because a
                    // long-running transaction exists.
                    warning!(
                        "pg_trickle: holdback probe failed ({}); holding at previous watermark",
                        e
                    );
                    let safe_lsn = match prev_watermark_lsn {
                        Some(prev) => {
                            // Re-use last known-safe watermark.
                            let u = version::lsn_to_u64(prev);
                            shmem::set_last_tick_safe_lsn(u);
                            Some(prev.to_string())
                        }
                        None => {
                            // First tick — no previous watermark; fall back to
                            // write LSN to avoid stalling forever on startup.
                            let lsn = Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text")
                                .unwrap_or(None);
                            if let Some(ref l) = lsn {
                                shmem::set_last_tick_safe_lsn(version::lsn_to_u64(l));
                            }
                            lsn
                        }
                    };
                    shmem::update_holdback_metrics(0, 0);
                    (safe_lsn, 0, 0)
                }
            }
        }

        config::FrontierHoldbackMode::LsnBytes(offset_bytes) => {
            let write_lsn_str = Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text")
                .unwrap_or(None)
                .unwrap_or_else(|| "0/0".to_string());
            let write_u64 = version::lsn_to_u64(&write_lsn_str);
            let safe_u64 = write_u64.saturating_sub(offset_bytes);
            let safe_lsn = version::u64_to_lsn(safe_u64);
            shmem::set_last_tick_safe_lsn(safe_u64);
            shmem::update_holdback_metrics(offset_bytes.min(write_u64), 0);
            (Some(safe_lsn), 0, 0)
        }
    }
}

/// Compute the tick watermark for a **dynamic refresh worker**.
///
/// Dynamic workers run after the coordinator and do not have access to
/// the previous tick's `prev_watermark_lsn`. They read the coordinator-
/// computed safe watermark from shared memory and cap it with the current
/// write LSN (in case the worker starts significantly after the tick).
///
/// When holdback is disabled or shmem is unavailable, falls back to
/// `pg_current_wal_lsn()`.
pub(super) fn compute_worker_tick_watermark() -> Option<String> {
    if !config::pg_trickle_tick_watermark_enabled() {
        return None;
    }

    let mode = config::pg_trickle_frontier_holdback_mode();

    match mode {
        config::FrontierHoldbackMode::None => {
            match Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text") {
                Ok(v) => v,
                Err(e) => {
                    pgrx::warning!(
                        "pg_trickle scheduler: failed to fetch pg_current_wal_lsn (worker/None): {}",
                        e
                    );
                    None
                }
            }
        }

        config::FrontierHoldbackMode::Xmin
        | config::FrontierHoldbackMode::LsnBytes(_)
        | config::FrontierHoldbackMode::InvalidLsn => {
            // Read the safe watermark the coordinator stored in shmem.
            let safe_lsn_u64 = shmem::last_tick_safe_lsn_u64();

            if safe_lsn_u64 == 0 {
                // No coordinator value yet — fall back to raw write LSN.
                return match Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text") {
                    Ok(v) => v,
                    Err(e) => {
                        pgrx::warning!(
                            "pg_trickle scheduler: failed to fetch pg_current_wal_lsn (worker/fallback): {}",
                            e
                        );
                        None
                    }
                };
            }

            // Cap with current write LSN: don't advance past what's available now.
            let write_lsn_str = match Spi::get_one::<String>("SELECT pg_current_wal_lsn()::text") {
                Ok(Some(v)) => v,
                Ok(None) => "0/0".to_string(),
                Err(e) => {
                    pgrx::warning!(
                        "pg_trickle scheduler: failed to fetch pg_current_wal_lsn (worker/cap): {}",
                        e
                    );
                    "0/0".to_string()
                }
            };
            let write_u64 = version::lsn_to_u64(&write_lsn_str);
            let effective_u64 = safe_lsn_u64.min(write_u64);
            Some(version::u64_to_lsn(effective_u64))
        }
    }
}

/// Rate-limited WARNING for when frontier holdback has been active longer
/// than `pg_trickle.frontier_holdback_warn_seconds`.
///
/// Emits at most one WARNING per minute.
pub(super) fn emit_holdback_warning_if_needed(oldest_txn_age_secs: u64) {
    let warn_secs = config::pg_trickle_frontier_holdback_warn_seconds();
    if warn_secs <= 0 {
        return;
    }
    if oldest_txn_age_secs < warn_secs as u64 {
        return;
    }

    // Rate-limit: emit at most once per minute.
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let last_warn = LAST_HOLDBACK_WARN_SECS.load(std::sync::atomic::Ordering::Relaxed);
    if now_secs.saturating_sub(last_warn) < 60 {
        return;
    }
    LAST_HOLDBACK_WARN_SECS.store(now_secs, std::sync::atomic::Ordering::Relaxed);

    pgrx::warning!(
        "pg_trickle: frontier holdback active — the oldest in-progress transaction is {}s old \
         (threshold: {}s). Stream tables may lag behind. \
         Check pg_stat_activity for long-running sessions. \
         To suppress: SET pg_trickle.frontier_holdback_warn_seconds = 0.",
        oldest_txn_age_secs,
        warn_secs,
    );
}
