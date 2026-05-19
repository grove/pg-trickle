//! API-1/2 (v0.62.0): Per-node scheduler pause/resume control.
//!
//! `pause_scheduler(nodes)` marks each named stream table as paused in shared
//! memory. The scheduler will skip dispatching refreshes for a paused node on
//! every subsequent tick. Once all named nodes are paused, the function waits
//! up to `pg_trickle.scheduler_drain_timeout` seconds for any in-flight refresh
//! workers to finish before returning.
//!
//! `resume_scheduler(nodes)` removes each named node from the paused set so
//! that the scheduler resumes dispatching refreshes normally on the next tick.

use pgrx::prelude::*;

use crate::catalog::StreamTableMeta;
use crate::config;
use crate::error::PgTrickleError;
use crate::shmem;

/// Parse a possibly schema-qualified name into `(schema, name)`.
///
/// Falls back to the session's `current_schema()` when no schema prefix is
/// present.
fn resolve_node_name(qualified: &str) -> Result<(String, String), PgTrickleError> {
    let parts: Vec<&str> = qualified.splitn(2, '.').collect();
    match parts.len() {
        1 => {
            let schema = Spi::get_one::<String>("SELECT current_schema()::text")
                .map_err(|e| PgTrickleError::SpiError(e.to_string()))?
                .unwrap_or_else(|| "public".to_string());
            Ok((schema, parts[0].to_string()))
        }
        2 => Ok((parts[0].to_string(), parts[1].to_string())),
        _ => Err(PgTrickleError::InvalidArgument(format!(
            "invalid stream table name: {qualified}"
        ))),
    }
}

/// Resolve a possibly-qualified stream table name to its `pgt_id`.
fn pgt_id_for_name(name: &str) -> Result<i64, PgTrickleError> {
    let (schema, table) = resolve_node_name(name)?;
    let meta = StreamTableMeta::get_by_name(&schema, &table)?;
    Ok(meta.pgt_id)
}

// ── SQL API ──────────────────────────────────────────────────────────────────

/// API-1 (v0.62.0): Pause the scheduler for the given stream table nodes.
///
/// Marks each node as paused in shared memory so that the scheduler skips
/// dispatching refreshes for it. After setting all pause flags, the call
/// polls `ACTIVE_REFRESH_WORKERS` every 100 ms up to
/// `pg_trickle.scheduler_drain_timeout` seconds.  If refresh workers are
/// still running at the timeout, a WARNING is logged and the function returns
/// (the nodes remain paused for future ticks).
///
/// Example:
/// ```sql
/// SELECT pgtrickle.pause_scheduler(ARRAY['public.my_view', 'analytics.summary']);
/// ```
#[pg_extern(schema = "pgtrickle")]
pub fn pause_scheduler(nodes: pgrx::Array<&str>) -> &'static str {
    let mut resolved: Vec<i64> = Vec::new();

    for maybe_name in nodes.iter() {
        match maybe_name {
            None => {
                pgrx::warning!("pg_trickle: pause_scheduler: NULL element ignored");
            }
            Some(name) => match pgt_id_for_name(name) {
                Ok(pgt_id) => {
                    shmem::pause_node(pgt_id);
                    resolved.push(pgt_id);
                    pgrx::log!(
                        "pg_trickle: pause_scheduler: node '{}' (pgt_id={}) marked as paused",
                        name,
                        pgt_id
                    );
                }
                Err(e) => {
                    pgrx::error!(
                        "pg_trickle: pause_scheduler: could not resolve '{}': {}",
                        name,
                        e
                    );
                }
            },
        }
    }

    if resolved.is_empty() {
        return "OK";
    }

    // Wait for any in-flight refresh workers to drain.
    let drain_timeout_secs = config::pg_trickle_scheduler_drain_timeout();
    let poll_interval = std::time::Duration::from_millis(100);
    let deadline =
        std::time::Instant::now() + std::time::Duration::from_secs(drain_timeout_secs as u64);

    loop {
        let active = shmem::active_worker_count();
        if active == 0 {
            break;
        }
        if std::time::Instant::now() >= deadline {
            pgrx::warning!(
                "pg_trickle: pause_scheduler: {} active refresh worker(s) still running after {}s drain timeout; \
                 the paused nodes will not be dispatched on future ticks",
                active,
                drain_timeout_secs,
            );
            break;
        }
        // Avoid sleeping inside a transaction — just do a short spin here.
        // pgrx pg_usleep is not available on all platforms; use std::thread::sleep.
        std::thread::sleep(poll_interval);
    }

    "OK"
}

/// API-2 (v0.62.0): Resume the scheduler for the given stream table nodes.
///
/// Removes each node from the paused set in shared memory. The scheduler will
/// resume dispatching refreshes for the node on the next tick.
///
/// Example:
/// ```sql
/// SELECT pgtrickle.resume_scheduler(ARRAY['public.my_view']);
/// ```
#[pg_extern(schema = "pgtrickle")]
pub fn resume_scheduler(nodes: pgrx::Array<&str>) -> &'static str {
    for maybe_name in nodes.iter() {
        match maybe_name {
            None => {
                pgrx::warning!("pg_trickle: resume_scheduler: NULL element ignored");
            }
            Some(name) => match pgt_id_for_name(name) {
                Ok(pgt_id) => {
                    shmem::resume_node(pgt_id);
                    pgrx::log!(
                        "pg_trickle: resume_scheduler: node '{}' (pgt_id={}) removed from paused set",
                        name,
                        pgt_id
                    );
                }
                Err(e) => {
                    pgrx::error!(
                        "pg_trickle: resume_scheduler: could not resolve '{}': {}",
                        name,
                        e
                    );
                }
            },
        }
    }

    "OK"
}
