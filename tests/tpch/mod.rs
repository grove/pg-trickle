//! Shared TPC-H test helpers.
//!
//! P2.11: Extracted from `e2e_tpch_tests.rs` and `e2e_tpch_dag_tests.rs` to
//! eliminate ~120 lines of duplication.  Both integration test binaries include
//! this module via `mod tpch;`.
//!
//! Because each integration-test file is its own compilation unit, the tpch
//! module is compiled independently for each binary.  `super::e2e::E2eDb`
//! refers to the `e2e` module declared by the parent test file.

#![allow(dead_code)] // some helpers are only used by one of the two test files

// ── Configuration ───────────────────────────────────────────────────────

/// Scale factor for TPC-H data generation.
pub fn scale_factor() -> f64 {
    std::env::var("TPCH_SCALE")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0.01)
}

/// Number of refresh cycles per query.
pub fn cycles() -> usize {
    std::env::var("TPCH_CYCLES")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
}

// ── Scale-factor dimensions ─────────────────────────────────────────────

pub fn sf_orders() -> usize {
    ((scale_factor() * 150_000.0) as usize).max(1_500)
}
pub fn sf_customers() -> usize {
    ((scale_factor() * 15_000.0) as usize).max(150)
}
pub fn sf_suppliers() -> usize {
    ((scale_factor() * 1_000.0) as usize).max(10)
}
pub fn sf_parts() -> usize {
    ((scale_factor() * 20_000.0) as usize).max(200)
}

/// Number of rows per RF cycle (INSERT/DELETE/UPDATE batch size).
pub fn rf_count() -> usize {
    let sf = scale_factor();
    let orders = ((sf * 150_000.0) as usize).max(1_500);
    (orders / 100).max(10)
}

// ── SQL file embedding ──────────────────────────────────────────────────
//
// Paths are relative to this file (tests/tpch/mod.rs), so bare filenames
// refer to files in tests/tpch/.

pub const SCHEMA_SQL: &str = include_str!("schema.sql");
pub const DATAGEN_SQL: &str = include_str!("datagen.sql");
pub const RF1_SQL: &str = include_str!("rf1.sql");
pub const RF2_SQL: &str = include_str!("rf2.sql");
pub const RF3_SQL: &str = include_str!("rf3.sql");

// ── Token substitution ──────────────────────────────────────────────────

/// Replace scale-factor tokens in a SQL template.
pub fn substitute_sf(sql: &str) -> String {
    sql.replace("__SF_ORDERS__", &sf_orders().to_string())
        .replace("__SF_CUSTOMERS__", &sf_customers().to_string())
        .replace("__SF_SUPPLIERS__", &sf_suppliers().to_string())
        .replace("__SF_PARTS__", &sf_parts().to_string())
}

/// Replace RF tokens in a mutation SQL template.
pub fn substitute_rf(sql: &str, next_orderkey: usize) -> String {
    sql.replace("__RF_COUNT__", &rf_count().to_string())
        .replace("__NEXT_ORDERKEY__", &next_orderkey.to_string())
        .replace("__SF_CUSTOMERS__", &sf_customers().to_string())
        .replace("__SF_PARTS__", &sf_parts().to_string())
        .replace("__SF_SUPPLIERS__", &sf_suppliers().to_string())
}

// ── Database helpers ────────────────────────────────────────────────────
//
// These functions reference `super::e2e::E2eDb` which resolves to the `e2e`
// module declared by the test file that includes this module.

use super::e2e::E2eDb;

/// Execute each semicolon-delimited SQL statement in `sql`, skipping blank
/// and comment-only segments.  Panics on the first error.
pub async fn exec_sql(db: &E2eDb, sql: &str) {
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.execute(stmt).await;
        }
    }
}

/// Execute each semicolon-delimited SQL statement, returning the first error.
pub async fn try_exec_sql(db: &E2eDb, sql: &str) -> Result<(), String> {
    for stmt in sql.split(';') {
        let stmt = stmt.trim();
        let has_sql = stmt.lines().any(|l| {
            let l = l.trim();
            !l.is_empty() && !l.starts_with("--")
        });
        if has_sql {
            db.try_execute(stmt).await.map_err(|e| e.to_string())?;
        }
    }
    Ok(())
}

/// Load the TPC-H schema into the database.
pub async fn load_schema(db: &E2eDb) {
    exec_sql(db, SCHEMA_SQL).await;
}

/// Load TPC-H data at the configured scale factor.
pub async fn load_data(db: &E2eDb) {
    let sql = substitute_sf(DATAGEN_SQL);
    exec_sql(db, &sql).await;
}

/// Return the current maximum `o_orderkey` value (used to generate non-conflicting RF1 keys).
pub async fn max_orderkey(db: &E2eDb) -> usize {
    let max: i64 = db
        .query_scalar("SELECT COALESCE(MAX(o_orderkey), 0)::bigint FROM orders")
        .await;
    max as usize
}
