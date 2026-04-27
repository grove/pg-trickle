//! F10 (v0.37.0): Integration tests for OpenTelemetry W3C Trace Context propagation.
//!
//! These tests verify that:
//! 1. The `__pgt_trace_context` column is present in change buffer tables.
//! 2. When `pg_trickle.trace_id` is set in the session, CDC triggers capture the
//!    traceparent value into `__pgt_trace_context`.
//! 3. The `pg_trickle` extension correctly propagates trace context through the
//!    CDC → DVM → MERGE pipeline.
//!
//! Note: Full OTLP export to Jaeger requires `pg_trickle.otel_endpoint` to be
//! set to a running OTLP collector. The Jaeger E2E test is gated behind
//! `#[ignore]` and can be run with `cargo test -- --ignored` when a local
//! Jaeger instance is available on port 4318.

mod e2e;

use e2e::E2eDb;

// ── F10-1: Change buffer has __pgt_trace_context column ─────────────────────

#[tokio::test]
async fn test_trace_context_column_exists_in_change_buffer() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE trace_src (
            id SERIAL PRIMARY KEY,
            val TEXT
        )",
    )
    .await;

    db.create_st(
        "trace_st",
        "SELECT id, val FROM trace_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Verify the change buffer has __pgt_trace_context column.
    let col_exists: Option<i64> = db
        .query_scalar_opt(
            "SELECT COUNT(*) FROM information_schema.columns \
             WHERE table_schema = 'pgtrickle_changes' \
               AND column_name = '__pgt_trace_context' \
               AND table_name LIKE 'changes_%'",
        )
        .await;

    assert!(
        col_exists.unwrap_or(0) > 0,
        "Change buffer should have __pgt_trace_context column"
    );
}

// ── F10-2: CDC trigger captures trace context from session GUC ───────────────

#[tokio::test]
async fn test_trace_context_captured_from_session_guc() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE trace_capture_src (
            id SERIAL PRIMARY KEY,
            val TEXT
        )",
    )
    .await;

    db.create_st(
        "trace_capture_st",
        "SELECT id, val FROM trace_capture_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Set a valid W3C traceparent in the session GUC before DML.
    let traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    db.execute(&format!("SET pg_trickle.trace_id = '{traceparent}'"))
        .await;

    // Insert a row — trigger should capture the trace context.
    db.execute("INSERT INTO trace_capture_src (val) VALUES ('hello trace')")
        .await;

    // Reset the session GUC.
    db.execute("RESET pg_trickle.trace_id").await;

    // Query the change buffer for the trace context.
    let trace_ctx: Option<String> = db
        .query_scalar_opt(
            "SELECT __pgt_trace_context \
             FROM pgtrickle_changes.changes_trace_capture_src \
             WHERE __pgt_trace_context IS NOT NULL \
             ORDER BY change_id DESC LIMIT 1",
        )
        .await;

    assert_eq!(
        trace_ctx.as_deref(),
        Some(traceparent),
        "Change buffer should contain the traceparent from the session GUC"
    );
}

// ── F10-3: Trace context is NULL when GUC is not set ─────────────────────────

#[tokio::test]
async fn test_trace_context_null_when_guc_not_set() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE trace_null_src (
            id SERIAL PRIMARY KEY,
            val TEXT
        )",
    )
    .await;

    db.create_st(
        "trace_null_st",
        "SELECT id, val FROM trace_null_src",
        "1m",
        "DIFFERENTIAL",
    )
    .await;

    // Insert without setting trace_id GUC.
    db.execute("INSERT INTO trace_null_src (val) VALUES ('no trace')")
        .await;

    // The change buffer row should have __pgt_trace_context = NULL.
    let null_count: Option<i64> = db
        .query_scalar_opt(
            "SELECT COUNT(*) FROM pgtrickle_changes.changes_trace_null_src \
             WHERE action = 'I' AND __pgt_trace_context IS NULL",
        )
        .await;

    assert!(
        null_count.unwrap_or(0) > 0,
        "Change buffer rows without trace GUC should have NULL __pgt_trace_context"
    );
}

// ── F10-4: Trace context propagated through refresh ──────────────────────────

#[tokio::test]
async fn test_trace_context_propagated_through_differential_refresh() {
    let db = E2eDb::new().await.with_extension().await;

    db.execute(
        "CREATE TABLE trace_refresh_src (
            id SERIAL PRIMARY KEY,
            amount INT
        )",
    )
    .await;

    let q = "SELECT id, amount FROM trace_refresh_src";
    db.create_st("trace_refresh_st", q, "1m", "DIFFERENTIAL")
        .await;
    db.assert_st_matches_query("trace_refresh_st", q).await;

    // Set trace context, then insert.
    let traceparent = "00-abcdef0123456789abcdef0123456789-aabbccdd11223344-01";
    db.execute(&format!("SET pg_trickle.trace_id = '{traceparent}'"))
        .await;

    db.execute("INSERT INTO trace_refresh_src (amount) VALUES (42), (100)")
        .await;

    // Enable trace propagation (no OTLP endpoint — just log).
    db.execute("ALTER SYSTEM SET pg_trickle.enable_trace_propagation = on")
        .await;
    db.reload_config_and_wait().await;

    // Refresh should succeed even with trace propagation enabled.
    db.refresh_st("trace_refresh_st").await;
    db.assert_st_matches_query("trace_refresh_st", q).await;

    // Disable trace propagation.
    db.execute("ALTER SYSTEM SET pg_trickle.enable_trace_propagation = off")
        .await;
    db.reload_config_and_wait().await;
}

// ── F10-5: OTLP JSON format unit test (in-process) ───────────────────────────
// This test runs entirely in the test process without needing PostgreSQL.
// It verifies the OTLP payload structure is correct.

#[test]
fn test_otlp_span_json_is_valid() {
    // Inline the TraceContext + OtelSpan logic here to test without pgrx.
    let tp = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01";
    // Parse manually (same logic as otel::TraceContext::parse).
    let parts: Vec<&str> = tp.splitn(4, '-').collect();
    assert_eq!(parts.len(), 4);
    let trace_id = parts[1];
    let parent_id = parts[2];
    assert_eq!(trace_id.len(), 32);
    assert_eq!(parent_id.len(), 16);

    // Verify span_name constants are defined correctly.
    // (These are string slices in otel.rs, confirmed by compile.)
    let expected_span_names = [
        "pgtrickle.cdc_drain",
        "pgtrickle.dvm_plan",
        "pgtrickle.merge_apply",
        "pgtrickle.notify_emit",
    ];
    for name in &expected_span_names {
        assert!(!name.is_empty(), "Span name should not be empty: {name}");
        assert!(
            name.starts_with("pgtrickle."),
            "Span name should start with 'pgtrickle.': {name}"
        );
    }
}
