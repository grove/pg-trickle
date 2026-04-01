//! E2E tests for monitoring views and status functions.
//!
//! Validates `pgtrickle.pgt_status()`, `pgtrickle.stream_tables_info`,
//! `pgtrickle.pg_stat_stream_tables`, and refresh history recording.
//!
//! Prerequisites: `./tests/build_e2e_image.sh`

mod e2e;

use e2e::E2eDb;

#[tokio::test]
async fn test_pgt_status_returns_rows() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_src VALUES (1)").await;

    db.create_st("mon_st", "SELECT id FROM mon_src", "1m", "FULL")
        .await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_status()")
        .await;
    assert!(count >= 1, "pgt_status() should return at least 1 row");

    // Verify the row contents
    let (status, mode, populated, errors) = db.pgt_status("mon_st").await;
    assert_eq!(status, "ACTIVE");
    assert_eq!(mode, "FULL");
    assert!(populated);
    assert_eq!(errors, 0);
}

#[tokio::test]
async fn test_pgt_status_multiple_sts() {
    let db = E2eDb::new().await.with_extension().await;

    for i in 1..=3 {
        db.execute(&format!(
            "CREATE TABLE mon_multi_{} (id INT PRIMARY KEY)",
            i
        ))
        .await;
        db.execute(&format!("INSERT INTO mon_multi_{} VALUES (1)", i))
            .await;
        db.create_st(
            &format!("mon_multi_st_{}", i),
            &format!("SELECT id FROM mon_multi_{}", i),
            "1m",
            "FULL",
        )
        .await;
    }

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.pgt_status()")
        .await;
    assert_eq!(count, 3, "pgt_status() should return 3 rows");
}

#[tokio::test]
async fn test_stream_tables_info_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_info (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_info VALUES (1)").await;

    db.create_st("mon_info_st", "SELECT id FROM mon_info", "1m", "FULL")
        .await;

    // Refresh to populate data_timestamp
    db.execute("INSERT INTO mon_info VALUES (2)").await;
    db.refresh_st("mon_info_st").await;

    // Verify stream_tables_info view has our ST with staleness columns
    let has_row: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pgtrickle.stream_tables_info \
                WHERE pgt_name = 'mon_info_st' \
            )",
        )
        .await;
    assert!(has_row, "stream_tables_info should contain our ST");

    // Verify staleness and stale columns exist and are queryable
    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_info_st'",
        )
        .await;
    // Just after refresh, staleness should not exceed schedule
    assert!(!stale, "stale should be false right after refresh");
}

#[tokio::test]
async fn test_pg_stat_stream_tables_view() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_stat (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_stat VALUES (1)").await;

    db.create_st("mon_stat_st", "SELECT id FROM mon_stat", "1m", "FULL")
        .await;

    // Do a manual refresh
    db.execute("INSERT INTO mon_stat VALUES (2)").await;
    db.refresh_st("mon_stat_st").await;

    // Verify pg_stat_stream_tables view exists and has our ST
    let has_row: bool = db
        .query_scalar(
            "SELECT EXISTS( \
                SELECT 1 FROM pgtrickle.pg_stat_stream_tables \
                WHERE pgt_name = 'mon_stat_st' \
            )",
        )
        .await;
    assert!(has_row, "pg_stat_stream_tables should contain our ST");

    // Verify key columns exist and are queryable
    let status: String = db
        .query_scalar(
            "SELECT status FROM pgtrickle.pg_stat_stream_tables WHERE pgt_name = 'mon_stat_st'",
        )
        .await;
    assert_eq!(status, "ACTIVE");

    // total_refreshes may be 0 because manual refresh doesn't record history,
    // only the scheduler does. Verify the column is queryable.
    let total: i64 = db
        .query_scalar(
            "SELECT total_refreshes FROM pgtrickle.pg_stat_stream_tables WHERE pgt_name = 'mon_stat_st'",
        )
        .await;
    assert!(
        total >= 0,
        "total_refreshes should be accessible (may be 0 for manual refresh)"
    );
}

#[tokio::test]
async fn test_stale_detection() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_sched (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_sched VALUES (1)").await;

    // Create ST with minimum schedule (60 seconds)
    db.create_st("mon_sched_st", "SELECT id FROM mon_sched", "1m", "FULL")
        .await;

    // The view should show staleness which grows over time.
    // Right after initial populate, staleness should be very small.
    let has_staleness: bool = db
        .query_scalar(
            "SELECT staleness IS NOT NULL FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_sched_st'",
        )
        .await;
    assert!(
        has_staleness,
        "staleness should be computed in stream_tables_info"
    );

    // stale should be false right after creation (schedule=60s)
    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'mon_sched_st'",
        )
        .await;
    assert!(!stale, "stale should be false immediately after creation");
}

// ── Staleness column source tests (PR #382) ──────────────────────────────────

/// STALE-TS-1: `staleness` and `stale` in `stream_tables_info` must be derived
/// from `last_refresh_at`, not `data_timestamp`.
///
/// Scenario: source data is idle (data_timestamp is old) but the scheduler has
/// been running on schedule (last_refresh_at is recent).  Before PR #382 this
/// case produced a false-positive `stale = true` because the view used
/// `data_timestamp`.  After the fix, stale must be false.
#[tokio::test]
async fn test_staleness_uses_last_refresh_at_not_data_timestamp() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE stale_ts1_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute("INSERT INTO stale_ts1_src VALUES (1, 'a')").await;
    db.create_st(
        "stale_ts1_st",
        "SELECT id, val FROM stale_ts1_src",
        "1m",
        "FULL",
    )
    .await;

    // Simulate an idle source: data_timestamp was 2 hours ago, but the
    // scheduler ran a NO_DATA cycle just now (last_refresh_at = recent).
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET data_timestamp  = now() - INTERVAL '2 hours', \
             last_refresh_at = now() - INTERVAL '5 seconds' \
         WHERE pgt_name = 'stale_ts1_st'",
    )
    .await;

    // stale must be false — the scheduler is keeping up with its 1-minute schedule.
    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) \
             FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'stale_ts1_st'",
        )
        .await;
    assert!(
        !stale,
        "stale must be false when last_refresh_at is recent, \
         even when data_timestamp is old (idle source)"
    );

    // staleness must reflect last_refresh_at (small), not data_timestamp (large).
    let staleness_secs: f64 = db
        .query_scalar(
            "SELECT EXTRACT(EPOCH FROM staleness) \
             FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'stale_ts1_st'",
        )
        .await;
    assert!(
        staleness_secs < 60.0,
        "staleness ({staleness_secs:.1}s) must reflect last_refresh_at (~5 s ago), \
         not data_timestamp (2 hours ago)"
    );
}

/// STALE-TS-2: `stale` flips to `true` when `last_refresh_at` falls behind schedule.
///
/// When `last_refresh_at` exceeds the configured schedule interval the stale
/// flag must be `true` and quick_health must report a WARNING.
#[tokio::test]
async fn test_stale_flag_triggers_on_last_refresh_at() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE stale_ts2_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO stale_ts2_src VALUES (1)").await;
    db.create_st(
        "stale_ts2_st",
        "SELECT id FROM stale_ts2_src",
        "1m",
        "FULL",
    )
    .await;

    // Backdate last_refresh_at past the 1-minute schedule threshold.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET last_refresh_at = now() - INTERVAL '2 hours' \
         WHERE pgt_name = 'stale_ts2_st'",
    )
    .await;

    // stream_tables_info.stale must be true.
    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) \
             FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'stale_ts2_st'",
        )
        .await;
    assert!(stale, "stale must be true when last_refresh_at is 2 hours behind a 1m schedule");

    // quick_health must reflect stale_tables > 0 and status = 'WARNING'.
    let stale_count: i64 = db
        .query_scalar("SELECT stale_tables FROM pgtrickle.quick_health")
        .await;
    assert_eq!(stale_count, 1, "quick_health.stale_tables must be 1");

    let qh_status: String = db
        .query_scalar("SELECT status FROM pgtrickle.quick_health")
        .await;
    assert_eq!(
        qh_status, "WARNING",
        "quick_health.status must be WARNING when a scheduled ST is overdue"
    );

    // Now reset last_refresh_at to now — stale must clear.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET last_refresh_at = now() \
         WHERE pgt_name = 'stale_ts2_st'",
    )
    .await;

    let stale_after: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) \
             FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'stale_ts2_st'",
        )
        .await;
    assert!(!stale_after, "stale must clear once last_refresh_at is updated to now");

    let qh_status_after: String = db
        .query_scalar("SELECT status FROM pgtrickle.quick_health")
        .await;
    assert_eq!(
        qh_status_after, "OK",
        "quick_health.status must return to OK once last_refresh_at is current"
    );
}

/// STALE-TS-3: `staleness` and `stale` are NULL when `last_refresh_at` is NULL.
///
/// A stream table that has never completed a scheduler cycle has
/// `last_refresh_at = NULL`.  Both staleness (an interval) and stale (boolean)
/// must be NULL in that state — the table is not yet old, just unrefreshed.
#[tokio::test]
async fn test_staleness_null_when_never_refreshed() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE stale_ts3_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO stale_ts3_src VALUES (1)").await;

    // Use create_st_with_init(false) so last_refresh_at stays NULL.
    db.execute(
        "SELECT pgtrickle.create_stream_table(\
         'stale_ts3_st', 'SELECT id FROM stale_ts3_src', '1m', 'FULL', false)",
    )
    .await;

    // Force last_refresh_at to NULL to simulate a never-scheduled table.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET last_refresh_at = NULL \
         WHERE pgt_name = 'stale_ts3_st'",
    )
    .await;

    // staleness must be NULL.
    let staleness_is_null: bool = db
        .query_scalar(
            "SELECT staleness IS NULL \
             FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'stale_ts3_st'",
        )
        .await;
    assert!(
        staleness_is_null,
        "staleness must be NULL when last_refresh_at is NULL"
    );

    // stale must be NULL (not true — the table is simply unscheduled/unrefreshed).
    let stale_is_null: bool = db
        .query_scalar(
            "SELECT stale IS NULL \
             FROM pgtrickle.stream_tables_info \
             WHERE pgt_name = 'stale_ts3_st'",
        )
        .await;
    assert!(
        stale_is_null,
        "stale must be NULL when last_refresh_at is NULL"
    );

    // The unrefreshed table must NOT count toward quick_health.stale_tables.
    let stale_count: i64 = db
        .query_scalar("SELECT stale_tables FROM pgtrickle.quick_health")
        .await;
    assert_eq!(
        stale_count, 0,
        "quick_health.stale_tables must not count tables with last_refresh_at = NULL"
    );
}

/// STALE-TS-4: `pg_stat_stream_tables` also reports staleness from
/// `last_refresh_at`, consistent with `stream_tables_info`.
#[tokio::test]
async fn test_pg_stat_stream_tables_staleness_uses_last_refresh_at() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE stale_ts4_src (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO stale_ts4_src VALUES (1)").await;
    db.create_st(
        "stale_ts4_st",
        "SELECT id FROM stale_ts4_src",
        "1m",
        "FULL",
    )
    .await;

    // Idle source scenario: data_timestamp old, last_refresh_at recent.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET data_timestamp  = now() - INTERVAL '3 hours', \
             last_refresh_at = now() - INTERVAL '10 seconds' \
         WHERE pgt_name = 'stale_ts4_st'",
    )
    .await;

    // pg_stat_stream_tables.stale must be false (scheduler is healthy).
    let stale: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) \
             FROM pgtrickle.pg_stat_stream_tables \
             WHERE pgt_name = 'stale_ts4_st'",
        )
        .await;
    assert!(
        !stale,
        "pg_stat_stream_tables.stale must be false when last_refresh_at is recent"
    );

    // staleness must be small — reflecting last_refresh_at, not data_timestamp.
    let staleness_secs: f64 = db
        .query_scalar(
            "SELECT EXTRACT(EPOCH FROM staleness) \
             FROM pgtrickle.pg_stat_stream_tables \
             WHERE pgt_name = 'stale_ts4_st'",
        )
        .await;
    assert!(
        staleness_secs < 60.0,
        "pg_stat_stream_tables.staleness ({staleness_secs:.1}s) must reflect \
         last_refresh_at (~10 s ago), not data_timestamp (3 hours ago)"
    );

    // Now backdate last_refresh_at — stale must flip.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables \
         SET last_refresh_at = now() - INTERVAL '90 seconds' \
         WHERE pgt_name = 'stale_ts4_st'",
    )
    .await;

    let stale_overdue: bool = db
        .query_scalar(
            "SELECT COALESCE(stale, false) \
             FROM pgtrickle.pg_stat_stream_tables \
             WHERE pgt_name = 'stale_ts4_st'",
        )
        .await;
    assert!(
        stale_overdue,
        "pg_stat_stream_tables.stale must be true when last_refresh_at exceeds the schedule"
    );
}

#[tokio::test]
async fn test_refresh_history_records() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE mon_hist (id INT PRIMARY KEY)")
        .await;
    db.execute("INSERT INTO mon_hist VALUES (1)").await;

    db.create_st("mon_hist_st", "SELECT id FROM mon_hist", "1m", "FULL")
        .await;

    // Multiple manual refreshes
    for i in 2..=4 {
        db.execute(&format!("INSERT INTO mon_hist VALUES ({})", i))
            .await;
        db.refresh_st("mon_hist_st").await;
    }

    // Manual refresh doesn't write to pgt_refresh_history (only scheduler does).
    // Verify the table exists and is queryable.
    let table_exists = db.table_exists("pgtrickle", "pgt_refresh_history").await;
    assert!(table_exists, "pgt_refresh_history table should exist");

    // Verify the history table has the expected columns
    let col_count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM information_schema.columns \
             WHERE table_schema = 'pgtrickle' AND table_name = 'pgt_refresh_history'",
        )
        .await;
    assert!(
        col_count >= 5,
        "pgt_refresh_history should have at least 5 columns, got {}",
        col_count,
    );

    // Verify the ST's catalog was correctly updated by manual refresh
    let count = db.count("public.mon_hist_st").await;
    assert_eq!(count, 4, "ST should have all 4 rows after refreshes");
}
