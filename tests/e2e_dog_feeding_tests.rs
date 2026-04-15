mod e2e;
use e2e::E2eDb;

// ── DF-F3: E2E test — setup/teardown cycle ──────────────────────────────────

#[tokio::test]
async fn test_dog_feeding_setup_creates_five_stream_tables() {
    let db = E2eDb::new().await.with_extension().await;

    // Create a source table and a user ST so pgt_refresh_history has data.
    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10), (2, 20), (3, 30)")
        .await;
    db.create_st("user_st", "SELECT id, val FROM src", "1m", "AUTO")
        .await;
    db.refresh_st("user_st").await;

    // Setup dog-feeding.
    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    // Verify all five DF stream tables exist.
    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(
        count, 5,
        "setup_dog_feeding should create 5 DF stream tables"
    );
}

// ── STAB-1: setup_dog_feeding() idempotency ─────────────────────────────────

#[tokio::test]
async fn test_dog_feeding_setup_idempotent_three_calls() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Call setup 3 times — should not error or create duplicates.
    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;
    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;
    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(count, 5, "3× setup should still produce exactly 5 DF STs");
}

// ── DF-F5 + STAB-5: teardown + partial teardown ────────────────────────────

#[tokio::test]
async fn test_dog_feeding_teardown_drops_all_stream_tables() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    // Verify STs exist before teardown.
    let before: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(before, 5);

    // Teardown.
    db.execute("SELECT pgtrickle.teardown_dog_feeding()").await;

    let after: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(after, 0, "teardown should remove all DF STs");

    // User ST should still exist.
    let user_st: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'user_st'",
        )
        .await;
    assert_eq!(user_st, 1, "user ST should survive teardown");
}

#[tokio::test]
async fn test_dog_feeding_teardown_safe_with_partial_setup() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Setup, then manually drop one DF ST to simulate partial state.
    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;
    db.execute("SELECT pgtrickle.drop_stream_table('pgtrickle.df_anomaly_signals', true)")
        .await;

    // Teardown should succeed without errors even with missing DF ST.
    db.execute("SELECT pgtrickle.teardown_dog_feeding()").await;

    let count: i64 = db
        .query_scalar(
            "SELECT count(*) FROM pgtrickle.pgt_stream_tables
             WHERE pgt_schema = 'pgtrickle' AND pgt_name LIKE 'df_%'",
        )
        .await;
    assert_eq!(count, 0, "teardown should clean up remaining STs");
}

// ── UX-1: dog_feeding_status() ──────────────────────────────────────────────

#[tokio::test]
async fn test_dog_feeding_status_reports_all_five() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.dog_feeding_status()")
        .await;
    assert_eq!(count, 5, "dog_feeding_status should report 5 rows");

    // All should exist.
    let all_exist: bool = db
        .query_scalar("SELECT bool_and(exists) FROM pgtrickle.dog_feeding_status()")
        .await;
    assert!(all_exist, "all five DF STs should report exists = true");
}

#[tokio::test]
async fn test_dog_feeding_status_before_setup() {
    let db = E2eDb::new().await.with_extension().await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.dog_feeding_status()")
        .await;
    assert_eq!(
        count, 5,
        "dog_feeding_status should report 5 rows even before setup"
    );

    let any_exist: bool = db
        .query_scalar("SELECT bool_or(exists) FROM pgtrickle.dog_feeding_status()")
        .await;
    assert!(!any_exist, "no DF STs should exist before setup");
}

// ── TEST-2: Full create/refresh/teardown cycle ──────────────────────────────

#[tokio::test]
async fn test_dog_feeding_full_lifecycle() {
    let db = E2eDb::new().await.with_extension().await;

    // Create source data.
    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src SELECT g, g * 10 FROM generate_series(1, 100) g")
        .await;

    // Create a user ST and run some refreshes to populate history.
    db.create_st(
        "user_st",
        "SELECT id, sum(val) AS total FROM src GROUP BY id",
        "1m",
        "AUTO",
    )
    .await;

    for _ in 0..3 {
        db.execute("INSERT INTO src SELECT g + (SELECT max(id) FROM src), g * 10 FROM generate_series(1, 10) g")
            .await;
        db.refresh_st("user_st").await;
    }

    // Setup dog-feeding.
    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    // Verify status.
    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.dog_feeding_status() WHERE exists")
        .await;
    assert_eq!(count, 5);

    // Teardown.
    db.execute("SELECT pgtrickle.teardown_dog_feeding()").await;

    let count_after: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.dog_feeding_status() WHERE exists")
        .await;
    assert_eq!(count_after, 0);
}

// ── OPS-4: explain_dag() ────────────────────────────────────────────────────

#[tokio::test]
async fn test_explain_dag_includes_df_nodes_after_setup() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    let dag: String = db.query_scalar("SELECT pgtrickle.explain_dag()").await;

    assert!(dag.contains("graph TD"), "should be a Mermaid graph");
    assert!(
        dag.contains("df_efficiency_rolling"),
        "should include DF-1 node"
    );
    assert!(
        dag.contains("df_anomaly_signals"),
        "should include DF-2 node"
    );
    assert!(
        dag.contains("df_threshold_advice"),
        "should include DF-3 node"
    );
}

#[tokio::test]
async fn test_explain_dag_dot_format() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;

    let dag: String = db.query_scalar("SELECT pgtrickle.explain_dag('dot')").await;

    assert!(dag.contains("digraph dag"), "should be a DOT graph");
}

// ── OPS-3: scheduler_overhead() ─────────────────────────────────────────────

#[tokio::test]
async fn test_scheduler_overhead_returns_valid_row() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    let count: i64 = db
        .query_scalar("SELECT count(*) FROM pgtrickle.scheduler_overhead()")
        .await;
    assert_eq!(count, 1, "scheduler_overhead should return exactly 1 row");

    let total: i64 = db
        .query_scalar("SELECT total_refreshes_1h FROM pgtrickle.scheduler_overhead()")
        .await;
    assert!(total >= 1, "should have at least 1 refresh in history");
}

// ── DF-G1: dog_feeding_auto_apply GUC ───────────────────────────────────────

#[tokio::test]
async fn test_dog_feeding_auto_apply_guc_exists() {
    let db = E2eDb::new().await.with_extension().await;

    // Default should be 'off'.
    let value: String = db
        .query_scalar("SHOW pg_trickle.dog_feeding_auto_apply")
        .await;
    assert_eq!(value, "off", "default should be 'off'");

    // Should accept valid values.
    db.execute("SET pg_trickle.dog_feeding_auto_apply = 'threshold_only'")
        .await;
    let value: String = db
        .query_scalar("SHOW pg_trickle.dog_feeding_auto_apply")
        .await;
    assert_eq!(value, "threshold_only");
}

// ── PERF-1: Index on pgt_refresh_history(pgt_id, start_time) ────────────────

#[tokio::test]
async fn test_refresh_history_index_on_pgt_id_start_time() {
    let db = E2eDb::new().await.with_extension().await;

    let exists: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_indexes
                WHERE schemaname = 'pgtrickle'
                  AND tablename = 'pgt_refresh_history'
                  AND indexname = 'idx_hist_pgt_start'
            )",
        )
        .await;
    assert!(exists, "PERF-1 index idx_hist_pgt_start should exist");
}

// ── CORR-4: INSERT-only CDC trigger invariant ───────────────────────────────

#[tokio::test]
async fn test_cdc_insert_only_trigger_on_refresh_history() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY)").await;
    db.execute("INSERT INTO src VALUES (1)").await;
    db.create_st("user_st", "SELECT id FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    // Setup dog-feeding — this creates STs that reference pgt_refresh_history.
    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    // Verify that CDC triggers on pgt_refresh_history are INSERT-only.
    // tgtype bitmask: INSERT=2, DELETE=4, UPDATE=8, TRUNCATE=16
    // AFTER ROW INSERT = 2 | 1 (AFTER) | 0 (ROW) = 3 in some encodings,
    // but the key check is that UPDATE (8) and DELETE (4) bits are not set.
    let has_non_insert: bool = db
        .query_scalar(
            "SELECT EXISTS (
                SELECT 1 FROM pg_trigger
                WHERE tgrelid = 'pgtrickle.pgt_refresh_history'::regclass
                  AND tgname LIKE 'pg_trickle_cdc_%'
                  AND (tgtype & 12) != 0  -- bits 4 (DELETE) or 8 (UPDATE) set
            )",
        )
        .await;
    assert!(
        !has_non_insert,
        "CDC triggers on pgt_refresh_history should be INSERT-only (CORR-4)"
    );
}

// ── DF-D3: Control plane survives DF ST suspension ──────────────────────────

#[tokio::test]
async fn test_control_plane_survives_df_st_suspension() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute("CREATE TABLE src (id INT PRIMARY KEY, val INT)")
        .await;
    db.execute("INSERT INTO src VALUES (1, 10), (2, 20)").await;
    db.create_st("user_st", "SELECT id, val FROM src", "1m", "FULL")
        .await;
    db.refresh_st("user_st").await;

    db.execute("SELECT pgtrickle.setup_dog_feeding()").await;

    // Drop all DF STs — simulate suspension.
    db.execute("SELECT pgtrickle.teardown_dog_feeding()").await;

    // User ST should still refresh successfully.
    db.execute("INSERT INTO src VALUES (3, 30)").await;
    db.refresh_st("user_st").await;

    let count: i64 = db.count("user_st").await;
    assert_eq!(count, 3, "user ST should still work after DF teardown");
}
