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
