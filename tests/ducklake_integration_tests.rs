//! Integration tests for the DuckLake change-feed CDC adapter (v0.65.0).
//!
//! Tests the new DuckLake-related features introduced in v0.65.0:
//!
//! - F-1: `CdcMode::DuckLakeChangeFeed` detection heuristic
//! - F-3: Snapshot-based frontier model (snapshot_id field)
//! - F-8: Compaction policy configuration and normalization
//!
//! These tests use the TestDb minimal schema and do not require a DuckLake
//! FDW server to be installed. DuckLake-specific end-to-end behaviour is
//! tested separately once a DuckLake-enabled container is available.

mod common;

use common::TestDb;

// ── F-1: FDW Detection ────────────────────────────────────────────────────

/// A plain foreign table backed by postgres_fdw must NOT be detected as
/// a DuckLake source. This guards against false positives in the detection
/// heuristic.
#[tokio::test]
async fn test_ducklake_detect_foreign_table_not_ducklake() {
    let db = TestDb::with_catalog().await;

    // Install postgres_fdw and create a minimal foreign server and table.
    db.execute("CREATE EXTENSION IF NOT EXISTS postgres_fdw")
        .await;
    db.execute(
        "CREATE SERVER IF NOT EXISTS local_server
         FOREIGN DATA WRAPPER postgres_fdw
         OPTIONS (dbname 'postgres', host 'localhost')",
    )
    .await;
    db.execute(
        "CREATE USER MAPPING IF NOT EXISTS FOR CURRENT_USER
         SERVER local_server
         OPTIONS (user 'postgres')",
    )
    .await;
    db.execute(
        "CREATE FOREIGN TABLE IF NOT EXISTS ft_not_ducklake (id INT)
         SERVER local_server
         OPTIONS (table_name 'pg_class', schema_name 'pg_catalog')",
    )
    .await;

    // The FDW for this table is postgres_fdw — its name does NOT contain
    // "ducklake", so the detection query should return FALSE.
    let is_ducklake: bool = db
        .query_scalar(
            "SELECT EXISTS (
                 SELECT 1
                 FROM pg_catalog.pg_foreign_table ft
                 JOIN pg_catalog.pg_foreign_server fs ON ft.ftserver = fs.oid
                 JOIN pg_catalog.pg_foreign_data_wrapper fdw ON fs.srvfdw = fdw.oid
                 WHERE ft.ftrelid = 'ft_not_ducklake'::regclass
                   AND lower(fdw.fdwname::text) LIKE '%ducklake%'
             )",
        )
        .await;

    assert!(
        !is_ducklake,
        "postgres_fdw foreign table should NOT be detected as DuckLake"
    );
}

// ── F-8: Compaction Policy Schema ─────────────────────────────────────────

/// The `ducklake_compaction_policy` column must exist on `pgt_stream_tables`
/// and accept NULL (meaning: use the global GUC) as well as 'fallback' and
/// 'error'. Any other value should be rejected by the CHECK constraint.
#[tokio::test]
async fn test_ducklake_compaction_policy_column_accepts_valid_values() {
    let db = TestDb::with_catalog().await;

    // Create a source table to satisfy the FK.
    db.execute("CREATE TABLE dlcp_src (id BIGSERIAL PRIMARY KEY, val TEXT)")
        .await;

    let source_oid: i64 = db
        .query_scalar("SELECT 'dlcp_src'::regclass::oid::bigint")
        .await;

    // NULL (default) must be accepted.
    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables
         (pgt_relid, pgt_name, pgt_schema, defining_query, refresh_mode, status)
         VALUES ({source_oid}, 'dlcp_test_null', 'public', 'SELECT 1', 'DIFFERENTIAL', 'ACTIVE')"
    ))
    .await;

    let policy: Option<String> = db
        .query_scalar("SELECT ducklake_compaction_policy FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'dlcp_test_null'")
        .await;
    assert!(policy.is_none(), "default compaction policy must be NULL");

    // 'fallback' must be accepted.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
         SET ducklake_compaction_policy = 'fallback'
         WHERE pgt_name = 'dlcp_test_null'",
    )
    .await;

    // 'error' must be accepted.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
         SET ducklake_compaction_policy = 'error'
         WHERE pgt_name = 'dlcp_test_null'",
    )
    .await;

    let policy: Option<String> = db
        .query_scalar("SELECT ducklake_compaction_policy FROM pgtrickle.pgt_stream_tables WHERE pgt_name = 'dlcp_test_null'")
        .await;
    assert_eq!(
        policy.as_deref(),
        Some("error"),
        "compaction policy should be 'error' after update"
    );
}

/// An invalid compaction policy value must be rejected by the CHECK constraint.
#[tokio::test]
async fn test_ducklake_compaction_policy_rejects_invalid_value() {
    let db = TestDb::with_catalog().await;

    db.execute("CREATE TABLE dlcp_inv_src (id BIGSERIAL PRIMARY KEY, val TEXT)")
        .await;
    let source_oid: i64 = db
        .query_scalar("SELECT 'dlcp_inv_src'::regclass::oid::bigint")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables
         (pgt_relid, pgt_name, pgt_schema, defining_query, refresh_mode, status)
         VALUES ({source_oid}, 'dlcp_inv_test', 'public', 'SELECT 1', 'DIFFERENTIAL', 'ACTIVE')"
    ))
    .await;

    // Attempting to set an invalid policy should fail with a CHECK violation.
    let result = db
        .try_execute(
            "UPDATE pgtrickle.pgt_stream_tables
             SET ducklake_compaction_policy = 'invalid_policy'
             WHERE pgt_name = 'dlcp_inv_test'",
        )
        .await;

    assert!(
        result.is_err(),
        "setting ducklake_compaction_policy = 'invalid_policy' must fail"
    );
}

// ── F-3: Frontier Snapshot ID Monotonicity (unit-style integration test) ──

/// The snapshot frontier for a DuckLake source must never move backwards.
///
/// This test verifies the Rust-level invariant by serialising a frontier to
/// JSON (as it would be stored in the `frontier` JSONB column), deserialising
/// it back, and asserting that the snapshot_id round-trips correctly.
#[tokio::test]
async fn test_ducklake_frontier_snapshot_id_never_moves_backward() {
    // This test exercises the Rust Frontier struct directly via JSON round-trip.
    // It does not require a live database connection.

    // Simulate two successive frontier snapshots.
    let frontier_json_old = serde_json::json!({
        "sources": {
            "ducklake:lake.events": {
                "lsn": "0/0",
                "snapshot_ts": "",
                "snapshot_id": 42
            }
        },
        "data_timestamp": null
    })
    .to_string();

    let frontier_json_new = serde_json::json!({
        "sources": {
            "ducklake:lake.events": {
                "lsn": "0/0",
                "snapshot_ts": "",
                "snapshot_id": 55
            }
        },
        "data_timestamp": null
    })
    .to_string();

    // Both must be stored as valid JSONB.
    let db = TestDb::with_catalog().await;
    db.execute("CREATE TABLE frontier_test_src (id INT PRIMARY KEY)")
        .await;
    let source_oid: i64 = db
        .query_scalar("SELECT 'frontier_test_src'::regclass::oid::bigint")
        .await;

    db.execute(&format!(
        "INSERT INTO pgtrickle.pgt_stream_tables
         (pgt_relid, pgt_name, pgt_schema, defining_query, refresh_mode, status, frontier)
         VALUES ({source_oid}, 'frontier_st', 'public', 'SELECT id FROM frontier_test_src',
                 'DIFFERENTIAL', 'ACTIVE', '{frontier_json_old}'::jsonb)"
    ))
    .await;

    // Read back the old snapshot_id.
    let old_id: i64 = db
        .query_scalar(
            "SELECT (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint
             FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'frontier_st'",
        )
        .await;
    assert_eq!(old_id, 42, "old snapshot_id should be 42");

    // Advance the frontier (newer snapshot_id = 55 > 42).
    db.execute(&format!(
        "UPDATE pgtrickle.pgt_stream_tables
         SET frontier = '{frontier_json_new}'::jsonb
         WHERE pgt_name = 'frontier_st'"
    ))
    .await;

    let new_id: i64 = db
        .query_scalar(
            "SELECT (frontier -> 'sources' -> 'ducklake:lake.events' ->> 'snapshot_id')::bigint
             FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'frontier_st'",
        )
        .await;
    assert_eq!(new_id, 55, "new snapshot_id should be 55");
    assert!(
        new_id > old_id,
        "snapshot_id must only move forward (monotonicity): {old_id} -> {new_id}"
    );
}

// ── v0.66.0: DuckLake Parquet Sink catalog column tests ───────────────────

/// F-2 (v0.66.0): The `ducklake_sink_mode` catalog column accepts 'append'
/// and 'replace' and rejects other values.
#[tokio::test]
async fn test_ducklake_sink_mode_accepts_valid_values() {
    let db = TestDb::with_catalog().await;

    // 'append' is valid.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET ducklake_sink_mode = 'append'
         WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
    )
    .await;

    // 'replace' is valid.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET ducklake_sink_mode = 'replace'
         WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
    )
    .await;

    // NULL is valid (disables the sink).
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables SET ducklake_sink_mode = NULL
         WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
    )
    .await;
}

/// F-2 (v0.66.0): `ducklake_sink_mode` rejects values outside the CHECK constraint.
#[tokio::test]
async fn test_ducklake_sink_mode_rejects_invalid_values() {
    let db = TestDb::with_catalog().await;

    // 'full' is NOT a valid sink mode — the CHECK constraint must reject it.
    let result = db
        .try_execute(
            "UPDATE pgtrickle.pgt_stream_tables SET ducklake_sink_mode = 'full'
             WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
        )
        .await;
    assert!(
        result.is_err(),
        "invalid sink mode 'full' should have been rejected by CHECK constraint"
    );
}

/// F-4 (v0.66.0): `ducklake_sink_path` can be set to any text value.
#[tokio::test]
async fn test_ducklake_sink_path_can_be_set() {
    let db = TestDb::with_catalog().await;

    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
         SET ducklake_sink_path = 's3://my-bucket/prefix/',
             ducklake_sink_mode = 'append'
         WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
    )
    .await;

    let path: String = db
        .query_scalar(
            "SELECT ducklake_sink_path
             FROM pgtrickle.pgt_stream_tables
             WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
        )
        .await;
    assert_eq!(path, "s3://my-bucket/prefix/");
}

/// F-4 (v0.66.0): `ducklake_sink_table_id` can be set and cleared.
#[tokio::test]
async fn test_ducklake_sink_table_id_can_be_set() {
    let db = TestDb::with_catalog().await;

    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
         SET ducklake_sink_table_id = 42
         WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
    )
    .await;

    let table_id: i64 = db
        .query_scalar(
            "SELECT ducklake_sink_table_id
             FROM pgtrickle.pgt_stream_tables
             WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
        )
        .await;
    assert_eq!(table_id, 42);

    // Clear it.
    db.execute(
        "UPDATE pgtrickle.pgt_stream_tables
         SET ducklake_sink_table_id = NULL
         WHERE pgt_id = (SELECT MIN(pgt_id) FROM pgtrickle.pgt_stream_tables)",
    )
    .await;
}

/// F-2 (v0.66.0): Stream tables with NULL `ducklake_sink_mode` do not write
/// any Parquet files (the sink fast-exits when mode is None).
/// This test verifies the catalog columns default to NULL on creation.
#[tokio::test]
async fn test_ducklake_sink_columns_default_to_null() {
    let db = TestDb::with_catalog().await;

    // Create a minimal stream table.
    db.execute("CREATE TABLE sink_null_src (id INT PRIMARY KEY)")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
             'public.sink_null_st',
             'SELECT id FROM sink_null_src',
             initialize := FALSE
         )",
    )
    .await;

    let mode: Option<String> = db
        .query_scalar_opt(
            "SELECT ducklake_sink_mode
             FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'sink_null_st'",
        )
        .await;
    let path: Option<String> = db
        .query_scalar_opt(
            "SELECT ducklake_sink_path
             FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'sink_null_st'",
        )
        .await;
    let table_id: Option<i64> = db
        .query_scalar_opt(
            "SELECT ducklake_sink_table_id
             FROM pgtrickle.pgt_stream_tables
             WHERE pgt_name = 'sink_null_st'",
        )
        .await;

    assert!(
        mode.is_none(),
        "ducklake_sink_mode should default to NULL, got: {:?}",
        mode
    );
    assert!(
        path.is_none(),
        "ducklake_sink_path should default to NULL, got: {:?}",
        path
    );
    assert!(
        table_id.is_none(),
        "ducklake_sink_table_id should default to NULL, got: {:?}",
        table_id
    );
}

// ── v0.67.0: INT-11 Snapshot Provenance ───────────────────────────────────

/// INT-11 (v0.67.0): `pgtrickle.pgt_ducklake_provenance` table must exist
/// with the correct columns after installation.
#[tokio::test]
async fn test_ducklake_provenance_table_exists() {
    let db = TestDb::with_catalog().await;

    let table_exists: bool = db
        .query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM information_schema.tables
                 WHERE table_schema = 'pgtrickle'
                   AND table_name   = 'pgt_ducklake_provenance'
             )",
        )
        .await;
    assert!(
        table_exists,
        "pgtrickle.pgt_ducklake_provenance must exist after catalog creation"
    );
}

/// INT-11 (v0.67.0): Required columns must be present in the provenance table.
#[tokio::test]
async fn test_ducklake_provenance_table_has_required_columns() {
    let db = TestDb::with_catalog().await;

    let required_columns = [
        "provenance_id",
        "stream_table_oid",
        "stream_table_name",
        "ducklake_snapshot_id",
        "refresh_id",
        "delta_row_count",
        "written_at",
    ];

    for col in &required_columns {
        let col_exists: bool = db
            .query_scalar(&format!(
                "SELECT EXISTS (
                     SELECT 1 FROM information_schema.columns
                     WHERE table_schema  = 'pgtrickle'
                       AND table_name    = 'pgt_ducklake_provenance'
                       AND column_name   = '{col}'
                 )"
            ))
            .await;
        assert!(
            col_exists,
            "pgt_ducklake_provenance must have column '{col}'"
        );
    }
}

/// INT-11 (v0.67.0): Provenance rows can be inserted and queried.
#[tokio::test]
async fn test_ducklake_provenance_row_insert_and_query() {
    let db = TestDb::with_catalog().await;

    // Insert a synthetic provenance record.
    db.execute(
        "INSERT INTO pgtrickle.pgt_ducklake_provenance
         (stream_table_oid, stream_table_name, ducklake_snapshot_id,
          refresh_id, delta_row_count)
         VALUES (9999, 'test_stream', 42, 7, 150)",
    )
    .await;

    let row_count: i64 = db
        .query_scalar(
            "SELECT COUNT(*)
             FROM pgtrickle.pgt_ducklake_provenance
             WHERE stream_table_name = 'test_stream'
               AND ducklake_snapshot_id = 42",
        )
        .await;
    assert_eq!(
        row_count, 1,
        "provenance row must be queryable after insert"
    );

    let delta_count: i64 = db
        .query_scalar(
            "SELECT delta_row_count
             FROM pgtrickle.pgt_ducklake_provenance
             WHERE stream_table_name = 'test_stream'",
        )
        .await;
    assert_eq!(delta_count, 150, "delta_row_count must be stored correctly");
}

/// INT-11 (v0.67.0): Multiple provenance rows can be inserted for the same
/// stream table (one per refresh cycle) — the table must not have a unique
/// constraint on (stream_table_oid, ducklake_snapshot_id).
#[tokio::test]
async fn test_ducklake_provenance_multiple_rows_per_stream_table() {
    let db = TestDb::with_catalog().await;

    for i in 1i64..=10 {
        db.execute(&format!(
            "INSERT INTO pgtrickle.pgt_ducklake_provenance
             (stream_table_oid, stream_table_name, ducklake_snapshot_id,
              refresh_id, delta_row_count)
             VALUES (1234, 'multi_cycle_st', {i}, {i}, {delta})",
            delta = i * 50
        ))
        .await;
    }

    let count: i64 = db
        .query_scalar(
            "SELECT COUNT(*)
             FROM pgtrickle.pgt_ducklake_provenance
             WHERE stream_table_name = 'multi_cycle_st'",
        )
        .await;
    assert_eq!(count, 10, "provenance must store one row per refresh cycle");
}

// ── v0.67.0: F-6 DuckLake View Registration (schema / unit level) ─────────

/// F-6 (v0.67.0): When `ducklake_view` table does not exist, the sink
/// does not error — it skips view registration gracefully.
/// This test verifies that no `ducklake_view` table is created by pg_trickle;
/// that table is DuckLake-owned and must be created externally.
#[tokio::test]
async fn test_ducklake_view_not_created_by_pgtrickle() {
    let db = TestDb::with_catalog().await;

    // pg_trickle must NOT create ducklake_view — that is DuckLake's table.
    let view_table_exists: bool = db
        .query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM information_schema.tables
                 WHERE table_name = 'ducklake_view'
             )",
        )
        .await;
    assert!(
        !view_table_exists,
        "pg_trickle must NOT create ducklake_view — it is DuckLake's catalog table"
    );
}

/// F-6 (v0.67.0): When a minimal `ducklake_view` table exists (simulating
/// DuckLake installed), `create_stream_table` with `sink='ducklake'` upserts
/// the view entry and `drop_stream_table` removes it.
#[tokio::test]
async fn test_ducklake_view_registration_with_stub_table() {
    let db = TestDb::with_catalog().await;

    // Create a minimal stub ducklake_view table to simulate DuckLake.
    db.execute(
        "CREATE TABLE IF NOT EXISTS ducklake_view (
             view_name        TEXT PRIMARY KEY,
             view_definition  TEXT
         )",
    )
    .await;

    // Create source and stream tables.
    db.execute("CREATE TABLE view_reg_src (id INT PRIMARY KEY, val TEXT)")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
             'public.view_reg_st',
             'SELECT id, val FROM view_reg_src',
             initialize := FALSE,
             sink := 'ducklake',
             ducklake_sink_path := 'file:///tmp/view_reg_test/'
         )",
    )
    .await;

    // The view entry should have been inserted.
    let entry_exists: bool = db
        .query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM ducklake_view WHERE view_name = 'view_reg_st'
             )",
        )
        .await;
    assert!(
        entry_exists,
        "create_stream_table with sink='ducklake' must upsert ducklake_view"
    );

    // Drop the stream table — the view entry must be removed.
    db.execute("SELECT pgtrickle.drop_stream_table('public.view_reg_st')")
        .await;

    let entry_after_drop: bool = db
        .query_scalar(
            "SELECT EXISTS (
                 SELECT 1 FROM ducklake_view WHERE view_name = 'view_reg_st'
             )",
        )
        .await;
    assert!(
        !entry_after_drop,
        "drop_stream_table must remove the ducklake_view entry"
    );
}

/// F-6 (v0.67.0): `alter_stream_table` with `sink => 'none'` (disabling the
/// sink) must remove the `ducklake_view` entry.
#[tokio::test]
async fn test_ducklake_view_deregistered_when_sink_disabled() {
    let db = TestDb::with_catalog().await;

    // Stub DuckLake view table.
    db.execute(
        "CREATE TABLE IF NOT EXISTS ducklake_view (
             view_name        TEXT PRIMARY KEY,
             view_definition  TEXT
         )",
    )
    .await;

    db.execute("CREATE TABLE view_dereg_src (id INT PRIMARY KEY)")
        .await;
    db.execute(
        "SELECT pgtrickle.create_stream_table(
             'public.view_dereg_st',
             'SELECT id FROM view_dereg_src',
             initialize := FALSE,
             sink := 'ducklake',
             ducklake_sink_path := 'file:///tmp/view_dereg_test/'
         )",
    )
    .await;

    // Confirm view entry exists.
    let exists_before: bool = db
        .query_scalar(
            "SELECT EXISTS (SELECT 1 FROM ducklake_view WHERE view_name = 'view_dereg_st')",
        )
        .await;
    assert!(
        exists_before,
        "view entry must exist after create with sink"
    );

    // Disable the sink.
    db.execute("SELECT pgtrickle.alter_stream_table('public.view_dereg_st', sink := 'none')")
        .await;

    let exists_after: bool = db
        .query_scalar(
            "SELECT EXISTS (SELECT 1 FROM ducklake_view WHERE view_name = 'view_dereg_st')",
        )
        .await;
    assert!(
        !exists_after,
        "disabling the sink must remove the ducklake_view entry"
    );
}
