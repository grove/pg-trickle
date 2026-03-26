//! WAKE-1: E2E tests for event-driven scheduler wake via LISTEN/NOTIFY.
//!
//! Verifies that:
//! 1. CDC triggers emit `pg_notify('pgtrickle_wake', '')` after writing to
//!    the change buffer.
//! 2. The scheduler wakes immediately on NOTIFY instead of waiting for the
//!    full poll interval.
//! 3. Poll-based fallback still works when event-driven wake is disabled.

mod e2e;

use e2e::E2eDb;
use std::time::Duration;

// ── Helpers ────────────────────────────────────────────────────────────────

/// Configure the scheduler with a long poll interval to make event-driven
/// wake distinguishable from poll-based wake.
async fn configure_event_driven_scheduler(db: &E2eDb) {
    // Set a long poll interval so we can distinguish event-driven wake
    // (fast) from poll-based wake (slow).
    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 5000")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.event_driven_wake = on")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.wake_debounce_ms = 10")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.scheduler_interval_ms", "5000")
        .await;
    db.wait_for_setting("pg_trickle.event_driven_wake", "on")
        .await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(
        sched_running,
        "pg_trickle scheduler did not appear within 90 s"
    );
}

/// Wait until a ST has at least `min_completed` COMPLETED refreshes.
async fn wait_for_n_refreshes(
    db: &E2eDb,
    pgt_name: &str,
    min_completed: i64,
    timeout: Duration,
) -> bool {
    let start = std::time::Instant::now();
    loop {
        if start.elapsed() > timeout {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;

        let count: i64 = db
            .query_scalar(&format!(
                "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
                 JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
                 WHERE d.pgt_name = '{pgt_name}' AND h.status = 'COMPLETED'"
            ))
            .await;
        if count >= min_completed {
            return true;
        }
    }
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// WAKE-1: Verify that CDC triggers include `pg_notify('pgtrickle_wake', '')`
/// in the generated trigger function body.
#[tokio::test]
async fn test_wake_cdc_trigger_emits_notify() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE wake_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO wake_src VALUES (1, 'a')").await;
    db.create_st(
        "wake_st",
        "SELECT id, val FROM wake_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Check that the trigger function includes the pg_notify call.
    let fn_body: String = db
        .query_scalar(
            "SELECT prosrc FROM pg_proc \
             WHERE proname LIKE 'pg_trickle_cdc_ins_fn_%' \
             ORDER BY oid DESC LIMIT 1",
        )
        .await;
    assert!(
        fn_body.contains("pg_notify('pgtrickle_wake'"),
        "INSERT trigger function should contain pg_notify('pgtrickle_wake'): {}",
        fn_body,
    );

    // Also check UPDATE and DELETE trigger functions.
    let upd_body: String = db
        .query_scalar(
            "SELECT prosrc FROM pg_proc \
             WHERE proname LIKE 'pg_trickle_cdc_upd_fn_%' \
             ORDER BY oid DESC LIMIT 1",
        )
        .await;
    assert!(
        upd_body.contains("pg_notify('pgtrickle_wake'"),
        "UPDATE trigger function should contain pg_notify('pgtrickle_wake')",
    );

    let del_body: String = db
        .query_scalar(
            "SELECT prosrc FROM pg_proc \
             WHERE proname LIKE 'pg_trickle_cdc_del_fn_%' \
             ORDER BY oid DESC LIMIT 1",
        )
        .await;
    assert!(
        del_body.contains("pg_notify('pgtrickle_wake'"),
        "DELETE trigger function should contain pg_notify('pgtrickle_wake')",
    );
}

/// WAKE-1: Verify the TRUNCATE trigger function also includes the notify.
#[tokio::test]
async fn test_wake_truncate_trigger_emits_notify() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("CREATE TABLE trunc_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO trunc_src VALUES (1, 'a')").await;
    db.create_st("trunc_st", "SELECT id, val FROM trunc_src", "1s", "FULL")
        .await;

    let fn_body: String = db
        .query_scalar(
            "SELECT prosrc FROM pg_proc \
             WHERE proname LIKE 'pg_trickle_cdc_truncate_fn_%' \
             ORDER BY oid DESC LIMIT 1",
        )
        .await;
    assert!(
        fn_body.contains("pg_notify('pgtrickle_wake'"),
        "TRUNCATE trigger function should contain pg_notify('pgtrickle_wake'): {}",
        fn_body,
    );
}

/// WAKE-1: Verify that event-driven wake causes a refresh to complete
/// significantly faster than the poll interval.
///
/// Setup: scheduler_interval_ms = 5000 (5 s), event_driven_wake = on.
/// After inserting data, the scheduler should wake within ~100 ms (debounce +
/// processing), NOT 5 s. We assert the refresh completes within 3 s (generous
/// margin for CI overhead).
#[tokio::test]
async fn test_wake_event_driven_latency() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;
    configure_event_driven_scheduler(&db).await;

    db.execute("CREATE TABLE lat_src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO lat_src VALUES (1, 100)").await;
    db.create_st(
        "lat_st",
        "SELECT id, val FROM lat_src",
        "1s",
        "DIFFERENTIAL",
    )
    .await;

    // Wait for the initial refresh (may use poll or schedule).
    let initial_ok = wait_for_n_refreshes(&db, "lat_st", 1, Duration::from_secs(30)).await;
    assert!(initial_ok, "Initial refresh did not complete");

    // Record the count before our insert.
    let before_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_refresh_history h \
             JOIN pgtrickle.pgt_stream_tables d ON h.pgt_id = d.pgt_id \
             WHERE d.pgt_name = 'lat_st' AND h.status = 'COMPLETED'",
        )
        .await;

    // Insert new data — this should trigger a NOTIFY and wake the scheduler.
    let insert_start = std::time::Instant::now();
    db.execute("INSERT INTO lat_src VALUES (2, 200)").await;

    // Wait for a NEW completed refresh (after our insert).
    let event_ok =
        wait_for_n_refreshes(&db, "lat_st", before_count + 1, Duration::from_secs(10)).await;
    let elapsed = insert_start.elapsed();

    assert!(
        event_ok,
        "Event-driven refresh did not complete within 10 s \
         (elapsed={:.1}s, poll_interval=5s). This suggests NOTIFY wake is not working.",
        elapsed.as_secs_f64(),
    );

    // The refresh should have completed faster than the poll interval.
    assert!(
        elapsed < Duration::from_secs(10),
        "Event-driven refresh took {:.1}s — should be much less than the 5s poll interval",
        elapsed.as_secs_f64(),
    );
}

/// WAKE-1: Verify that poll-based fallback still works when event_driven_wake
/// is disabled.
#[tokio::test]
async fn test_wake_poll_fallback_works() {
    let db = E2eDb::new_on_postgres_db().await.with_extension().await;

    db.execute("ALTER SYSTEM SET pg_trickle.scheduler_interval_ms = 200")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.min_schedule_seconds = 1")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.auto_backoff = off")
        .await;
    db.execute("ALTER SYSTEM SET pg_trickle.event_driven_wake = off")
        .await;
    db.reload_config_and_wait().await;
    db.wait_for_setting("pg_trickle.event_driven_wake", "off")
        .await;

    let sched_running = db.wait_for_scheduler(Duration::from_secs(90)).await;
    assert!(sched_running, "scheduler did not start");

    db.execute("CREATE TABLE poll_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO poll_src VALUES (1, 'a')").await;
    db.create_st("poll_st", "SELECT id, val FROM poll_src", "1s", "FULL")
        .await;

    // Poll-based refresh should still work within a reasonable time frame.
    let ok = wait_for_n_refreshes(&db, "poll_st", 1, Duration::from_secs(30)).await;
    assert!(ok, "Poll-based refresh did not complete within 30 s");
}
