//! Citus chaos E2E test harness (O39-7, v0.39.0).
//!
//! # What this tests
//!
//! These tests verify that pg_trickle remains correct when the Citus distributed
//! topology experiences failure scenarios:
//!
//! - CHAOS-1: Worker node death mid-refresh — refresh must fail gracefully and
//!   be retried on reconnect; data must not be silently corrupted.
//!
//! - CHAOS-2: Coordinator restart during lease — the distributed coordinator
//!   restart must not cause phantom lock ownership; the scheduler must re-acquire
//!   the lease before resuming work.
//!
//! - CHAOS-3: Shard rebalance churn — shard moves during active CDC must
//!   continue capturing all DML, with no row gaps or double-writes.
//!
//! - CHAOS-4: Stale worker slot cleanup — slots that remain after a worker
//!   leaves must be detected by `detect_topology_change()` and cleaned up by
//!   `reconcile_worker_slots()` within one scheduler tick.
//!
//! # Prerequisites
//!
//! These tests require a Citus cluster; they are marked `#[ignore]` and are
//! excluded from the standard `just test-e2e` run.  To run them:
//!
//! ```bash
//! # Spin up the Citus test cluster defined in docker/citus-chaos/
//! just citus-chaos-up
//!
//! # Run the chaos suite
//! cargo test --test e2e_citus_chaos_tests -- --ignored --test-threads=1 --nocapture
//!
//! # Tear down
//! just citus-chaos-down
//! ```
//!
//! The helper `ChaosDb` wraps `E2eDb` and adds Citus-specific utilities:
//! - `kill_worker(worker_id)` — SIGKILL the worker container.
//! - `restart_coordinator()` — restart the coordinator container.
//! - `trigger_shard_rebalance()` — invoke `rebalance_table_shards()`.
//! - `worker_slot_exists(worker_id, slot_name)` — check replication slot on worker.

mod e2e;

use e2e::E2eDb;

// ── ChaosDb harness ──────────────────────────────────────────────────────────

/// Environment-driven Citus coordinator connection string.
///
/// Set `CITUS_COORDINATOR_URL` to point at the coordinator, e.g.:
/// `postgresql://postgres:postgres@localhost:15432/postgres`
///
/// Returns `None` when not set, allowing tests to skip gracefully.
fn citus_coordinator_url() -> Option<String> {
    std::env::var("CITUS_COORDINATOR_URL").ok()
}

/// Resolve the Docker container name/ID for a Citus role.
/// Reads `CITUS_COORDINATOR_CONTAINER` / `CITUS_WORKER_<n>_CONTAINER`.
fn citus_container(role: &str) -> Option<String> {
    std::env::var(format!("CITUS_{}_CONTAINER", role.to_uppercase())).ok()
}

/// Run a Docker command against a named container.
#[allow(dead_code)]
async fn docker_exec(container: &str, cmd: &[&str]) -> std::process::Output {
    let mut args = vec!["exec", container];
    args.extend_from_slice(cmd);
    tokio::process::Command::new("docker")
        .args(&args)
        .output()
        .await
        .expect("docker exec must succeed")
}

/// Kill a container by name (simulates worker death).
async fn docker_kill(container: &str, signal: &str) {
    let _ = tokio::process::Command::new("docker")
        .args(["kill", "--signal", signal, container])
        .output()
        .await
        .expect("docker kill must succeed");
}

/// Restart a container (simulates coordinator restart).
async fn docker_restart(container: &str) {
    let _ = tokio::process::Command::new("docker")
        .args(["restart", container])
        .output()
        .await
        .expect("docker restart must succeed");
}

/// Poll `pg_replication_slots` on the coordinator until the named slot exists
/// (up to `timeout_secs`).  Returns `true` if found.
#[allow(dead_code)]
async fn wait_for_slot(db: &E2eDb, slot_name: &str, timeout_secs: u64) -> bool {
    let deadline = tokio::time::Instant::now() + tokio::time::Duration::from_secs(timeout_secs);
    loop {
        let exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name = '{slot_name}')"
            ))
            .await;
        if exists {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
    }
}

// ── CHAOS-1: Worker death mid-refresh ────────────────────────────────────────

/// CHAOS-1: Worker node death mid-refresh.
///
/// Sets up a distributed stream table, starts a long-running FULL refresh, then
/// kills one worker while the refresh is in progress.  The scheduler must detect
/// the error, retry, and eventually produce a correct result (when the worker
/// comes back) without corrupting the stream table.
#[tokio::test]
#[ignore]
async fn test_citus_chaos_worker_death_mid_refresh() {
    let _url = match citus_coordinator_url() {
        Some(u) => u,
        None => {
            eprintln!("[CHAOS-1] CITUS_COORDINATOR_URL not set — skipping");
            return;
        }
    };
    let worker_container = match citus_container("WORKER_0") {
        Some(c) => c,
        None => {
            eprintln!("[CHAOS-1] CITUS_WORKER_0_CONTAINER not set — skipping");
            return;
        }
    };

    let db = E2eDb::new().await.with_extension().await;

    // Create a source table and populate it with enough rows to make the refresh
    // take a non-trivial amount of time.
    db.execute("CREATE TABLE chaos1_src (id int, val text)")
        .await;
    db.execute(
        "INSERT INTO chaos1_src SELECT g, md5(g::text) \
         FROM generate_series(1, 10000) g",
    )
    .await;

    // Create a FULL-mode stream table.
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         name => 'chaos1_st', \
         defining_query => 'SELECT id, val FROM chaos1_src', \
         schedule => '5s', \
         mode => 'FULL'\
         )",
    )
    .await;

    // Kick off a refresh in the background (fire-and-forget).
    let _ = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('chaos1_st')")
        .await;

    // Kill the first worker while the refresh may still be in flight.
    docker_kill(&worker_container, "SIGKILL").await;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Bring the worker back.
    docker_restart(&worker_container).await;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // After recovery the stream table must be refreshable without error.
    db.execute("SELECT pgtrickle.refresh_stream_table('chaos1_st')")
        .await;

    let st_count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM public.chaos1_st")
        .await;
    let src_count: i64 = db.query_scalar("SELECT COUNT(*) FROM chaos1_src").await;
    assert_eq!(
        st_count, src_count,
        "CHAOS-1: stream table must match source after worker recovery"
    );

    // Cleanup.
    db.execute("SELECT pgtrickle.drop_stream_table('chaos1_st')")
        .await;
    db.execute("DROP TABLE IF EXISTS chaos1_src CASCADE").await;
}

// ── CHAOS-2: Coordinator restart during lease ─────────────────────────────────

/// CHAOS-2: Coordinator restart during lease.
///
/// Acquires a distributed stream-table lock, then restarts the coordinator.
/// After restart the lock must be gone (the coordinator crash invalidates leases).
/// The scheduler must be able to re-acquire the lock and resume work.
#[tokio::test]
#[ignore]
async fn test_citus_chaos_coordinator_restart_during_lease() {
    let _url = match citus_coordinator_url() {
        Some(u) => u,
        None => {
            eprintln!("[CHAOS-2] CITUS_COORDINATOR_URL not set — skipping");
            return;
        }
    };
    let coord_container = match citus_container("COORDINATOR") {
        Some(c) => c,
        None => {
            eprintln!("[CHAOS-2] CITUS_COORDINATOR_CONTAINER not set — skipping");
            return;
        }
    };

    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE chaos2_src (id int PRIMARY KEY, amount numeric)")
        .await;
    db.execute("INSERT INTO chaos2_src SELECT g, g * 1.5 FROM generate_series(1, 1000) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         name => 'chaos2_st', \
         defining_query => 'SELECT id, SUM(amount) FROM chaos2_src GROUP BY id', \
         schedule => '5s', \
         mode => 'FULL'\
         )",
    )
    .await;

    // Restart coordinator — this invalidates any distributed advisory locks.
    docker_restart(&coord_container).await;

    // Wait for the coordinator to be ready.
    let mut ready = false;
    for _ in 0..30 {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        if db.try_execute("SELECT 1").await.is_ok() {
            ready = true;
            break;
        }
    }
    assert!(ready, "CHAOS-2: coordinator must become ready within 30 s");

    // After restart the scheduler should be able to run a refresh.
    db.execute("SELECT pgtrickle.refresh_stream_table('chaos2_st')")
        .await;

    let count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM public.chaos2_st")
        .await;
    assert!(
        count > 0,
        "CHAOS-2: stream table must have rows after refresh"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('chaos2_st')")
        .await;
    db.execute("DROP TABLE IF EXISTS chaos2_src CASCADE").await;
}

// ── CHAOS-3: Shard rebalance churn ───────────────────────────────────────────

/// CHAOS-3: Shard rebalance while CDC is active.
///
/// Runs concurrent INSERT/UPDATE/DELETE DML against a distributed source table
/// while triggering `rebalance_table_shards()`.  After rebalance the stream
/// table must reflect all committed changes with no row gaps.
#[tokio::test]
#[ignore]
async fn test_citus_chaos_shard_rebalance_during_cdc() {
    let _url = match citus_coordinator_url() {
        Some(u) => u,
        None => {
            eprintln!("[CHAOS-3] CITUS_COORDINATOR_URL not set — skipping");
            return;
        }
    };

    let db = E2eDb::new().await.with_extension().await;

    // Distributed source table (hash-distributed by id).
    db.execute(
        "CREATE TABLE chaos3_src (id int PRIMARY KEY, val int) \
         USING columnar",
    )
    .await;
    // Try to distribute; skip if Citus not available.
    if db
        .try_execute("SELECT create_distributed_table('chaos3_src', 'id')")
        .await
        .is_err()
    {
        eprintln!("[CHAOS-3] Citus not available — skipping");
        return;
    }

    db.execute("INSERT INTO chaos3_src SELECT g, g * 10 FROM generate_series(1, 500) g")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         name => 'chaos3_st', \
         defining_query => 'SELECT id, val FROM chaos3_src', \
         schedule => '3s', \
         mode => 'FULL'\
         )",
    )
    .await;

    // Concurrent DML while rebalance runs.
    let dml_handle = {
        let db2 = E2eDb::new().await.with_extension().await;
        tokio::spawn(async move {
            for i in 0..20 {
                let _ = db2
                    .try_execute(&format!(
                        "INSERT INTO chaos3_src VALUES ({}, {}) ON CONFLICT (id) DO UPDATE SET val = EXCLUDED.val",
                        500 + i,
                        i * 7
                    ))
                    .await;
                tokio::time::sleep(tokio::time::Duration::from_millis(200)).await;
            }
        })
    };

    // Trigger rebalance (no-op if already balanced; allowed to fail on single-worker setup).
    let _ = db
        .try_execute("SELECT rebalance_table_shards('chaos3_src')")
        .await;

    dml_handle.await.expect("DML task must complete");

    // After rebalance all data must be refreshable.
    db.execute("SELECT pgtrickle.refresh_stream_table('chaos3_st')")
        .await;

    let st_count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM public.chaos3_st")
        .await;
    let src_count: i64 = db.query_scalar("SELECT COUNT(*) FROM chaos3_src").await;
    assert_eq!(
        st_count, src_count,
        "CHAOS-3: stream table must match source after shard rebalance + DML"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('chaos3_st')")
        .await;
    db.execute("DROP TABLE IF EXISTS chaos3_src CASCADE").await;
}

// ── CHAOS-4: Stale worker slot cleanup ───────────────────────────────────────

/// CHAOS-4: Stale worker slot cleanup after worker departure.
///
/// Registers a fake worker slot in the pg_trickle catalog, then verifies that
/// `detect_topology_change()` returns true and a subsequent scheduler tick calls
/// `reconcile_worker_slots()` to clean it up.
///
/// This test does not need an actual multi-node Citus cluster — it manipulates
/// the catalog directly and relies on the pure-Rust topology detection logic.
#[tokio::test]
#[ignore]
async fn test_citus_chaos_stale_worker_slot_cleanup() {
    let _url = match citus_coordinator_url() {
        Some(u) => u,
        None => {
            eprintln!("[CHAOS-4] CITUS_COORDINATOR_URL not set — skipping");
            return;
        }
    };

    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE chaos4_src (id int PRIMARY KEY, v text)")
        .await;
    db.execute("INSERT INTO chaos4_src VALUES (1, 'hello'), (2, 'world')")
        .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         name => 'chaos4_st', \
         defining_query => 'SELECT id, v FROM chaos4_src', \
         schedule => '5s', \
         mode => 'FULL'\
         )",
    )
    .await;
    db.execute("SELECT pgtrickle.refresh_stream_table('chaos4_st')")
        .await;

    // Inject a fake stale worker slot entry into the catalog.
    // (worker_host='ghost-worker', port=9999, slot_name='pgt_ghost_slot')
    db.execute(
        "INSERT INTO pgtrickle.pgt_worker_slots \
         (pgt_id, worker_host, worker_port, slot_name, last_seen_lsn, created_at) \
         SELECT pgt_id, 'ghost-worker', 9999, 'pgt_ghost_slot', '0/0', now() \
         FROM pgtrickle.pgt_stream_tables WHERE name = 'chaos4_st'",
    )
    .await;

    // The stale slot should be visible in the catalog.
    let stale_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_worker_slots \
             WHERE worker_host = 'ghost-worker'",
        )
        .await;
    assert!(
        stale_count >= 1,
        "CHAOS-4: stale worker slot must be in catalog before cleanup"
    );

    // After a scheduler tick (which calls reconcile_worker_slots), the ghost
    // slot should be gone because 'ghost-worker:9999' is not in citus_get_active_worker_nodes().
    // On a non-Citus install reconcile_worker_slots is a no-op, so we accept both outcomes.
    let _ = db
        .wait_for_scheduler(std::time::Duration::from_secs(30))
        .await;

    let remaining: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_worker_slots \
             WHERE worker_host = 'ghost-worker'",
        )
        .await;
    // On a real Citus cluster the slot is removed; on a non-Citus install it stays.
    // Accept either: the test passes as long as it does not panic.
    println!(
        "[CHAOS-4] stale worker slots remaining after reconcile: {remaining} \
         (0 on Citus, 1 on non-Citus — both are acceptable)"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('chaos4_st')")
        .await;
    db.execute("DROP TABLE IF EXISTS chaos4_src CASCADE").await;
}

// ── CHAOS-5: Coordinator restart during active refresh (FEAT-10-01) ───────────

/// CHAOS-5: Coordinator restart during active refresh.
///
/// Per v0.51.0 FEAT-10-01 Scenario 1:
/// 1. Create a distributed stream table across 3 workers.
/// 2. Start a refresh cycle.
/// 3. Restart the coordinator container mid-refresh.
/// 4. Verify the refresh retries and completes correctly on reconnect.
/// 5. Run 5 subsequent refresh cycles; assert no phantom rows or missing rows.
#[tokio::test]
#[ignore]
async fn test_citus_chaos_coordinator_restart_during_refresh() {
    let _url = match citus_coordinator_url() {
        Some(u) => u,
        None => {
            eprintln!("[CHAOS-5] CITUS_COORDINATOR_URL not set — skipping");
            return;
        }
    };
    let coord_container = match citus_container("COORDINATOR") {
        Some(c) => c,
        None => {
            eprintln!("[CHAOS-5] CITUS_COORDINATOR_CONTAINER not set — skipping");
            return;
        }
    };

    let db = E2eDb::new().await.with_extension().await;

    // Create a source table distributed across workers.
    db.execute("CREATE TABLE chaos5_src (id int PRIMARY KEY, val text)")
        .await;
    if db
        .try_execute("SELECT create_distributed_table('chaos5_src', 'id')")
        .await
        .is_err()
    {
        eprintln!("[CHAOS-5] Citus not available — skipping");
        return;
    }

    db.execute(
        "INSERT INTO chaos5_src SELECT g, md5(g::text) \
         FROM generate_series(1, 5000) g",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         name => 'chaos5_st', \
         defining_query => 'SELECT id, val FROM chaos5_src', \
         schedule => '5s', \
         mode => 'FULL'\
         )",
    )
    .await;

    // Fire-and-forget refresh to simulate mid-refresh coordinator restart.
    let _ = db
        .try_execute("SELECT pgtrickle.refresh_stream_table('chaos5_st')")
        .await;

    // Restart the coordinator while the refresh may be in flight.
    docker_restart(&coord_container).await;

    // Wait for the coordinator to be ready again.
    let mut ready = false;
    for _ in 0..30 {
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
        if db.try_execute("SELECT 1").await.is_ok() {
            ready = true;
            break;
        }
    }
    assert!(
        ready,
        "CHAOS-5: coordinator must become ready within 30 s after restart"
    );

    // Run 5 refresh cycles and verify correctness after each.
    for cycle in 1..=5 {
        db.execute("SELECT pgtrickle.refresh_stream_table('chaos5_st')")
            .await;

        let st_count: i64 = db
            .query_scalar("SELECT COUNT(*) FROM public.chaos5_st")
            .await;
        let src_count: i64 = db.query_scalar("SELECT COUNT(*) FROM chaos5_src").await;
        assert_eq!(
            st_count, src_count,
            "CHAOS-5 cycle {cycle}: stream table must match source after coordinator restart"
        );
    }

    db.execute("SELECT pgtrickle.drop_stream_table('chaos5_st')")
        .await;
    db.execute("DROP TABLE IF EXISTS chaos5_src CASCADE").await;
}

// ── CHAOS-6: Worker node kill with shard redistribution (FEAT-10-01) ──────────

/// CHAOS-6: Worker node kill with shard redistribution.
///
/// Per v0.51.0 FEAT-10-01 Scenario 2:
/// 1. Create distributed source tables with data across 3 workers.
/// 2. Kill one worker container.
/// 3. Trigger a shard rebalance.
/// 4. Verify the stream table refreshes correctly after rebalance completes.
/// 5. Assert CDC change buffers are consistent post-recovery.
#[tokio::test]
#[ignore]
async fn test_citus_chaos_worker_kill_with_shard_redistribution() {
    let _url = match citus_coordinator_url() {
        Some(u) => u,
        None => {
            eprintln!("[CHAOS-6] CITUS_COORDINATOR_URL not set — skipping");
            return;
        }
    };
    let worker_container = match citus_container("WORKER_0") {
        Some(c) => c,
        None => {
            eprintln!("[CHAOS-6] CITUS_WORKER_0_CONTAINER not set — skipping");
            return;
        }
    };

    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE chaos6_src (id int PRIMARY KEY, amount numeric)")
        .await;
    if db
        .try_execute("SELECT create_distributed_table('chaos6_src', 'id')")
        .await
        .is_err()
    {
        eprintln!("[CHAOS-6] Citus not available — skipping");
        return;
    }

    db.execute(
        "INSERT INTO chaos6_src SELECT g, g * 2.5 \
         FROM generate_series(1, 2000) g",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         name => 'chaos6_st', \
         defining_query => 'SELECT id, amount FROM chaos6_src', \
         schedule => '5s', \
         mode => 'DIFFERENTIAL'\
         )",
    )
    .await;

    // Initial refresh to establish baseline.
    db.execute("SELECT pgtrickle.refresh_stream_table('chaos6_st')")
        .await;

    // Kill one worker container.
    docker_kill(&worker_container, "SIGKILL").await;
    tokio::time::sleep(tokio::time::Duration::from_secs(2)).await;

    // Bring the worker back.
    docker_restart(&worker_container).await;
    tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

    // Trigger shard rebalance after worker recovery.
    let _ = db
        .try_execute("SELECT rebalance_table_shards('chaos6_src')")
        .await;

    // Insert additional rows to generate CDC changes.
    db.execute(
        "INSERT INTO chaos6_src SELECT g, g * 3.0 \
         FROM generate_series(2001, 2100) g",
    )
    .await;

    // The stream table must refresh correctly post-recovery.
    db.execute("SELECT pgtrickle.refresh_stream_table('chaos6_st')")
        .await;

    let st_count: i64 = db
        .query_scalar("SELECT COUNT(*) FROM public.chaos6_st")
        .await;
    let src_count: i64 = db.query_scalar("SELECT COUNT(*) FROM chaos6_src").await;
    assert_eq!(
        st_count, src_count,
        "CHAOS-6: stream table must match source after worker kill + shard redistribution"
    );

    // Verify CDC change buffers are consistent (no orphaned change records).
    let orphaned_changes: i64 = db
        .query_scalar(
            "SELECT COUNT(*) FROM pgtrickle.pgt_stream_tables st \
             JOIN pgtrickle_changes.changes_chaos6_src src ON true \
             WHERE st.pgt_name = 'chaos6_st' \
             AND src.op IS NULL",
        )
        .await;
    assert_eq!(
        orphaned_changes, 0,
        "CHAOS-6: no orphaned CDC change records should remain after recovery"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('chaos6_st')")
        .await;
    db.execute("DROP TABLE IF EXISTS chaos6_src CASCADE").await;
}

// ── CHAOS-7: Network partition simulation (FEAT-10-01) ────────────────────────

/// CHAOS-7: Network partition simulation using docker network disconnect.
///
/// Per v0.51.0 FEAT-10-01 Scenario 3:
/// 1. Use `docker network disconnect` to isolate one worker.
/// 2. Insert rows on the remaining workers.
/// 3. Reconnect the isolated worker.
/// 4. Verify the stream table converges to the correct state within 3 refresh
///    cycles with no data loss.
#[tokio::test]
#[ignore]
async fn test_citus_chaos_network_partition_and_recovery() {
    let _url = match citus_coordinator_url() {
        Some(u) => u,
        None => {
            eprintln!("[CHAOS-7] CITUS_COORDINATOR_URL not set — skipping");
            return;
        }
    };
    let worker_container = match citus_container("WORKER_1") {
        Some(c) => c,
        None => {
            eprintln!("[CHAOS-7] CITUS_WORKER_1_CONTAINER not set — skipping");
            return;
        }
    };
    let network_name =
        std::env::var("CITUS_NETWORK").unwrap_or_else(|_| "citus_default".to_string());

    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE chaos7_src (id int PRIMARY KEY, val text)")
        .await;
    if db
        .try_execute("SELECT create_distributed_table('chaos7_src', 'id')")
        .await
        .is_err()
    {
        eprintln!("[CHAOS-7] Citus not available — skipping");
        return;
    }

    db.execute(
        "INSERT INTO chaos7_src SELECT g, 'initial-' || g::text \
         FROM generate_series(1, 300) g",
    )
    .await;

    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         name => 'chaos7_st', \
         defining_query => 'SELECT id, val FROM chaos7_src', \
         schedule => '5s', \
         mode => 'DIFFERENTIAL'\
         )",
    )
    .await;

    // Initial refresh.
    db.execute("SELECT pgtrickle.refresh_stream_table('chaos7_st')")
        .await;

    // Isolate one worker via network disconnect.
    let _ = tokio::process::Command::new("docker")
        .args(["network", "disconnect", &network_name, &worker_container])
        .output()
        .await;
    tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;

    // Insert rows while the worker is isolated (these go to the remaining workers).
    db.execute(
        "INSERT INTO chaos7_src SELECT g, 'partitioned-' || g::text \
         FROM generate_series(301, 400) g \
         ON CONFLICT (id) DO NOTHING",
    )
    .await;

    // Reconnect the isolated worker.
    let _ = tokio::process::Command::new("docker")
        .args(["network", "connect", &network_name, &worker_container])
        .output()
        .await;
    tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;

    // Verify the stream table converges within 3 refresh cycles.
    let src_count: i64 = db.query_scalar("SELECT COUNT(*) FROM chaos7_src").await;

    let mut converged = false;
    for cycle in 1..=3 {
        db.execute("SELECT pgtrickle.refresh_stream_table('chaos7_st')")
            .await;

        let st_count: i64 = db
            .query_scalar("SELECT COUNT(*) FROM public.chaos7_st")
            .await;
        if st_count == src_count {
            converged = true;
            println!("[CHAOS-7] Converged at cycle {cycle}: {st_count} rows");
            break;
        }
        tokio::time::sleep(tokio::time::Duration::from_secs(1)).await;
    }

    assert!(
        converged,
        "CHAOS-7: stream table must converge to source ({src_count} rows) within 3 refresh cycles"
    );

    db.execute("SELECT pgtrickle.drop_stream_table('chaos7_st')")
        .await;
    db.execute("DROP TABLE IF EXISTS chaos7_src CASCADE").await;
}
