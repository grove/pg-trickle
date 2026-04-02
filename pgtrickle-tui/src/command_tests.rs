//! Contract tests for every CLI command's `execute()` function.
//!
//! ## Why these tests exist
//!
//! The `commands/*.rs` files call `tokio_postgres` queries against
//! `pgtrickle.*` SQL functions.  Previously, the only tests were CLI
//! argument-parsing tests (`cli.rs`), which never touched a database.
//! That meant bugs like wrong column names or missing functions went
//! undetected until the binary was run against a real database.
//!
//! Each test here:
//!   1. Spins up a fresh Postgres 18 container via Testcontainers.
//!   2. Installs minimal stub functions whose signatures match the real
//!      extension (correct column names and types).
//!   3. Calls the command's `execute()` with realistic arguments.
//!   4. Asserts the call succeeds (or, for 0.14.0-only features, fails
//!      with the expected "requires >= 0.14.0" message).
//!
//! If a stub signature drifts from what the command file reads — wrong
//! column name, wrong type, wrong positional index — the test fails here
//! rather than at runtime against a production database.

use crate::cli::OutputFormat;
use crate::commands;
use crate::test_db::PgtStubDb;

// ── list ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_list_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::list::ListArgs {
        format: OutputFormat::Table,
    };
    commands::list::execute(&db.client, &args)
        .await
        .expect("list: execute() failed");
}

// ── status ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_status_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::status::StatusArgs {
        name: "test_table".into(),
        format: OutputFormat::Table,
    };
    // Stub returns a row with pgt_name='test_table', so this should succeed
    commands::status::execute(&db.client, &args)
        .await
        .expect("status: execute() failed");
}

#[tokio::test]
async fn test_cmd_status_not_found_returns_error() {
    let db = PgtStubDb::new().await;
    let args = commands::status::StatusArgs {
        name: "nonexistent_table".into(),
        format: OutputFormat::Table,
    };
    // Stub only returns 'test_table'; a different name yields empty result → NotFound
    let err = commands::status::execute(&db.client, &args)
        .await
        .expect_err("status: should return NotFound for unknown table");
    assert!(
        matches!(err, crate::error::CliError::NotFound(_)),
        "expected NotFound, got: {err}"
    );
}

// ── cdc ───────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_cdc_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::cdc::CdcArgs {
        format: OutputFormat::Table,
    };
    commands::cdc::execute(&db.client, &args)
        .await
        .expect("cdc: execute() failed");
}

// ── graph ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_graph_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::graph::GraphArgs {
        format: OutputFormat::Table,
    };
    commands::graph::execute(&db.client, &args)
        .await
        .expect("graph: execute() failed");
}

// ── health ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_health_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::health::HealthArgs {
        format: OutputFormat::Table,
    };
    commands::health::execute(&db.client, &args)
        .await
        .expect("health: execute() failed");
}

// ── config ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_config_list_executes() {
    let db = PgtStubDb::new().await;
    // No pg_trickle GUCs in a vanilla Postgres — expect empty table, not an error
    let args = commands::config::ConfigArgs {
        set: None,
        format: OutputFormat::Table,
    };
    commands::config::execute(&db.client, &args)
        .await
        .expect("config list: execute() failed");
}

// ── fuse ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_fuse_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::fuse::FuseArgs {
        format: OutputFormat::Table,
    };
    commands::fuse::execute(&db.client, &args)
        .await
        .expect("fuse: execute() failed");
}

// ── watermarks ────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_watermarks_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::watermarks::WatermarksArgs {
        format: OutputFormat::Table,
    };
    commands::watermarks::execute(&db.client, &args)
        .await
        .expect("watermarks: execute() failed");
}

// ── workers ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_workers_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::workers::WorkersArgs {
        format: OutputFormat::Table,
    };
    commands::workers::execute(&db.client, &args)
        .await
        .expect("workers: execute() failed");
}

// ── explain ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_explain_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::explain::ExplainArgs {
        name: "test_table".into(),
        analyze: false,
        format: OutputFormat::Table,
    };
    commands::explain::execute(&db.client, &args)
        .await
        .expect("explain: execute() failed");
}

#[tokio::test]
async fn test_cmd_explain_analyze_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::explain::ExplainArgs {
        name: "test_table".into(),
        analyze: true,
        format: OutputFormat::Table,
    };
    commands::explain::execute(&db.client, &args)
        .await
        .expect("explain --analyze: execute() failed");
}

// ── diag ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_diag_all_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::diag::DiagArgs {
        name: None,
        format: OutputFormat::Table,
    };
    commands::diag::execute(&db.client, &args)
        .await
        .expect("diag (all): execute() failed");
}

#[tokio::test]
async fn test_cmd_diag_named_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::diag::DiagArgs {
        name: Some("test_table".into()),
        format: OutputFormat::Table,
    };
    commands::diag::execute(&db.client, &args)
        .await
        .expect("diag (named): execute() failed");
}

// ── export ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_export_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::export::ExportArgs {
        name: "test_table".into(),
    };
    commands::export::execute(&db.client, &args)
        .await
        .expect("export: execute() failed");
}

// ── create ────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_create_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::create::CreateArgs {
        name: "my_stream".into(),
        query: "SELECT 1 AS id".into(),
        schedule: None,
        mode: None,
        no_initialize: false,
    };
    commands::create::execute(&db.client, &args)
        .await
        .expect("create: execute() failed");
}

// ── drop ──────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_drop_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::drop::DropArgs {
        name: "my_stream".into(),
    };
    commands::drop::execute(&db.client, &args)
        .await
        .expect("drop: execute() failed");
}

// ── refresh ───────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_refresh_all_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::refresh::RefreshArgs {
        name: None,
        all: true,
    };
    commands::refresh::execute(&db.client, &args)
        .await
        .expect("refresh --all: execute() failed");
}

#[tokio::test]
async fn test_cmd_refresh_named_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::refresh::RefreshArgs {
        name: Some("test_table".into()),
        all: false,
    };
    commands::refresh::execute(&db.client, &args)
        .await
        .expect("refresh <name>: execute() failed");
}

// ── alter ─────────────────────────────────────────────────────────────────

#[tokio::test]
async fn test_cmd_alter_mode_executes() {
    let db = PgtStubDb::new().await;
    let args = commands::alter::AlterArgs {
        name: "my_stream".into(),
        mode: Some("full".into()),
        schedule: None,
        tier: None,
        status: None,
        query: None,
    };
    commands::alter::execute(&db.client, &args)
        .await
        .expect("alter --mode: execute() failed");
}

#[tokio::test]
async fn test_cmd_alter_no_flags_returns_error() {
    let db = PgtStubDb::new().await;
    // Calling alter with no flags is an error (nothing to change)
    let args = commands::alter::AlterArgs {
        name: "my_stream".into(),
        mode: None,
        schedule: None,
        tier: None,
        status: None,
        query: None,
    };
    commands::alter::execute(&db.client, &args)
        .await
        .expect_err("alter with no flags should return an error");
}

// ── output formats ────────────────────────────────────────────────────────
// Verify JSON and CSV output paths don't panic

#[tokio::test]
async fn test_cmd_list_json_format() {
    let db = PgtStubDb::new().await;
    let args = commands::list::ListArgs {
        format: OutputFormat::Json,
    };
    commands::list::execute(&db.client, &args)
        .await
        .expect("list --format json: execute() failed");
}

#[tokio::test]
async fn test_cmd_fuse_csv_format() {
    let db = PgtStubDb::new().await;
    let args = commands::fuse::FuseArgs {
        format: OutputFormat::Csv,
    };
    commands::fuse::execute(&db.client, &args)
        .await
        .expect("fuse --format csv: execute() failed");
}

#[tokio::test]
async fn test_cmd_workers_json_format() {
    let db = PgtStubDb::new().await;
    let args = commands::workers::WorkersArgs {
        format: OutputFormat::Json,
    };
    commands::workers::execute(&db.client, &args)
        .await
        .expect("workers --format json: execute() failed");
}
